package com.chen.guo.kafka

class UDFWithException {
  def throwException(num: Int): Int = {
    if (num < 10) {
      return num
    }
    throw new RuntimeException(s"Got unexpected number $num")
  }
}