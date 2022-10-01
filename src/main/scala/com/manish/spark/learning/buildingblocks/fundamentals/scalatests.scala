package com.manish.spark.learning.buildingblocks.fundamentals

object scalatests extends App {

   val l = List(
      (Some("Manish"), Some("Hello")),
      (None, Some("friend"))
   )
   val pairs = l.collect{
        case (name, Some(greet)) => (name, greet)
     }
   println(pairs)
}
