package com.aura.bigdata

import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class CollectionTest {
    @Test def collectionTest(): Unit = {
        val ab = ArrayBuffer[Int](0, 2, 3, 4, 5, 6, 7, 8)

        val ab1 = ab.drop(1)
        println(ab1)
        println(ab)

        println(ab.dropRight(1))

        println(ab.dropWhile(num => num % 2 != 0))

    }
}
