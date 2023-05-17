package org.arg.demos

object Hello {
  def main(args: Array[String]): Unit = {
    println("Hello Polamalu..!")
    val ages = Seq(42, 75, 29, 64)
    println(s"The oldest person is ${ages.max}")

    // add 5 seconds delay
    Thread.sleep(5000)
  }
}