package com.aura.bigdata

import java.io.{BufferedWriter, FileWriter}
import java.util.Random

import org.junit.Test

class FileTest {

    @Test def testFile(): Unit = {
        val array = Array(
            "hello you",
            "hello me",
            "hello hei",
            "hello you"
        )
        val bw = new BufferedWriter(new FileWriter("E:/data/spark/streaming/test/heihei.txt"))
        val random = new Random()
        for (i <- 0 until(10)) {
            bw.write(array(random.nextInt(array.length)))
            bw.newLine()
        }
        bw.close()
    }
}
