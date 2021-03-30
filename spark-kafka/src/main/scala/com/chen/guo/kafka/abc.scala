package com.chen.guo.kafka

object abc {
  def run1(a: Unit): Unit = {
  }

  def run2(a: () => Unit): Unit = {
    a.apply()
  }

  def main(args: Array[String]): Unit = {
    run1({
      println("123") //will be printed
    })

    run1(() => {
      println("abc") //will not be executed
    })

    run2(() => {
      println("efg") //will be printed. Won't be printed if "a.apply()" is commented out
    })
  }
}
