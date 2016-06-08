package com.sourcelearning.dataguru
import java.io.{PrintWriter}
import java.net.ServerSocket
import scala.io.Source
//销售模拟器  销售模拟器：参数1：读入的文件；参数2：端口；参数3：发送时间间隔ms
object SaleSimulation {
  def index(length: Int) = {
    import java.util.Random
    val rdm = new Random

    rdm.nextInt(length)
  }
//参数1：读入的文件；参数2：端口；参数3：发送时间间隔ms
  def main(args: Array[String]) {
//    if (args.length != 3) {
//      System.err.println("Usage: <filename> <port> <millisecond>")
//      System.exit(1)
//    }

    val filename = "/tmp/test1/people.txt"
    val port = 9000
    val threadTime =1000
   //filename 是参数1  把文件读进来 存在一个list里面
    val lines = Source.fromFile(filename).getLines.toList
  //总共有多少行
    val filerow = lines.length
//开启一个socket的listener
    val listener = new ServerSocket(port)
    while (true) {
      //一旦有其它客户端进行连接
      val socket = listener.accept()
      new Thread() {
        //开始发送数据
        override def run = {
          println("Got client connected from: " + socket.getInetAddress)
          val out = new PrintWriter(socket.getOutputStream(), true)
          while (true) {
            //间隔多少时间
            Thread.sleep(threadTime.toLong)
            //随机的调用文件的第几行
            val content = lines(index(filerow))
//            println(content)
            out.write(content + '\n')
            out.flush()
          }
          socket.close()
        }
      }.start()
    }
  }
}
