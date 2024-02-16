# from pyflink.common import Row
# from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
# from pyflink.common.typeinfo import Types
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


import json
import sys
from pyflink.common import Row
from pyflink.common.serialization import SimpleStringSchema, SerializationSchema,Encoder
from pyflink.common.typeinfo import Types,BasicType,TypeInformation,BasicTypeInfo
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext,MapFunction, KeyedBroadcastProcessFunction
from pyflink.datastream.connectors.kafka import KafkaSource,KafkaOffsetsInitializer
from pyflink.common import SimpleStringSchema, WatermarkStrategy
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, MapStateDescriptor


class Transaction:
  def __init__(self, uid: str, price: float):
    self.uid = uid
    self.price = price
  @classmethod
  def fromString(cls, inp: str):
    tmp = json.loads(inp)
    try:
        uid = tmp.get("uid")
        price = tmp.get("price")
        return cls(uid,price)
    except:
        raise Exception("Input string is malformed")


  def toDict(self) -> dict :
    return self.__dict__
  
  def toJson(self) -> str :
    return json.dumps(self.toDict())



class MyKeyedBroadcastProcessFunction(KeyedBroadcastProcessFunction):

    def __init__(self):
        self.currentStateDescriptor = ValueStateDescriptor("CurrentState", Types.PICKLED_BYTE_ARRAY())
        self.initialStateDescriptor = MapStateDescriptor("InitialState", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        self.currentState = None
    
    def open(self, ctx: RuntimeContext):
        self.currentState = ctx.get_state(self.currentStateDescriptor)
    
    def process_broadcast_element(self, value, ctx: KeyedBroadcastProcessFunction.Context):
        ctx.get_broadcast_state(self.initialStateDescriptor).put(value[0], value)
    
    def process_element(self, transaction, ctx: KeyedBroadcastProcessFunction.ReadOnlyContext):
        currentUid = transaction[0]
        currentState = self.currentState.value()
        if currentState is None:
            initialState = ctx.get_broadcast_state(self.initialStateDescriptor).get(currentUid)
            if initialState is None:
                currentState = (currentUid,0,0)
            else:
                currentState = initialState
        newCount = currentState[1] + 1
        newTotal = currentState[2] + transaction[1]
        self.currentState.update((currentUid,newCount,newTotal))
        yield currentUid,newCount,newTotal



class StringToTransaction(MapFunction):
    def map(self, value: str):
       try:
          tmp = Transaction.fromString(value)
          return (tmp.uid, tmp.price)
       except:
          return "Input string malformed:"+ value


def broadcast_state():
    
    env = StreamExecutionEnvironment.get_execution_environment()
    env.add_jars("file:///C:\\kafka_2.13-3.6.1\\libs\\flink-sql-connector-kafka-3.0.2-1.18.jar")


    kafka_source = KafkaSource.builder() \
    .set_bootstrap_servers("localhost:9092") \
    .set_topics("transactions") \
    .set_group_id("my-group") \
    .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

    kafka_input = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "Kafka Source").name("Reading Kafka topic").map(StringToTransaction()).name("String -> Transaction serialisation") \
    .key_by(lambda row: row[0]) \


    side_input = env.from_collection([("uid_1",1,10),("uid_2",2,20)]) #.key_by(lambda row: row[0]).flat_map(CountWindowAverage())
    userProfileStateDescription = MapStateDescriptor("InitialState", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
    initialState = side_input.broadcast(userProfileStateDescription)

    output = kafka_input \
    .connect(initialState) \
    .process(MyKeyedBroadcastProcessFunction()).print()




    env.execute()

    
    


if __name__ == '__main__':
    broadcast_state()
