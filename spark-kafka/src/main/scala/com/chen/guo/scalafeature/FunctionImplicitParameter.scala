package com.chen.guo.scalafeature


class Animal(val name: String) {
  override def toString: String = name
}

class Wrapper[T](id: Int, inner: T) {
  override def toString = s"${id}: ${inner}"
}

object ImplicitObjects {
  implicit val w: Wrapper[Animal] = new Wrapper[Animal](2, new Animal("inner animal2"))
}

object FunctionImplicitParameter {
  def main(args: Array[String]): Unit = {
    print(List(new Animal("cat")))
    println()
    println()

    //Option 1: explicitly pass the implicit variable
    val w1: Wrapper[Animal] = new Wrapper[Animal](1, new Animal("inner animal"))
    print2(List(new Animal("cat")))(w1)

    //Option 2: define an implicit variable
    implicit val w2: Wrapper[Animal] = new Wrapper[Animal](2, new Animal("inner animal2"))
    val thisIsWhatIsPassedIn = implicitly[Wrapper[Animal]]
    print2(List(new Animal("cat")))

    // Option 3: import the implicit objects from other locations
    //    import ImplicitObjects._
    //    print2(List(new Animal("cat")))

  }

  def print[T](x: List[T]): Unit = {
    println("In print")
    //    val what: Wrapper[T] = implicitly[Wrapper[T]]
    //    println(what)
    println(x)
  }

  /**
    * This syntax [T: Wrapper] explicitly asks for an implicit parameter of Wrapper[T]
    * More Examples
    * https://github.com/apache/spark/blob/master/sql/core/src/main/scala/org/apache/spark/sql/SQLImplicits.scala#L232
    *
    * Read more
    * https://stackoverflow.com/questions/53430546/supplying-implicit-argument-explicitly
    * https://stackoverflow.com/questions/3213510/what-is-a-manifest-in-scala-and-when-do-you-need-it/3213914#3213914
    *
    */
  def print2[T: Wrapper](x: List[T]): Unit = {
    println("In print2")
    val implicitlySummoned: Wrapper[T] = implicitly[Wrapper[T]]
    println(s"Got implicit object: ${implicitlySummoned}")
    println(x)
  }
}
