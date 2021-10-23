import org.apache.flink.streaming.api.scala._

object Scala_WordCount {
  def main(args: Array[String]): Unit = {
   val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env
      .socketTextStream("localhost",9999)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()
  }
}
