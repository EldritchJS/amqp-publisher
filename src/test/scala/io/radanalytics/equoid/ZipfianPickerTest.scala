package io.radanalytics.equoid

import io.radanalytics.equoid.DataPublisher._
import org.scalatest._

class ZipfianPicker extends FlatSpec with Matchers {

  val seed = 42

  behavior of "ZipfianPicker for numbers"

  "the most frequent element out of 20 numbers" should "be picked ~(3 +- 2) times" in {
    val picker = ZipfianPicker[String]("file:///" + getClass.getResource("/numbers.txt").getPath, seed)

    val sortedByFreq = ((for (elem <- picker) yield elem)
      .toVector
      .groupBy(identity)
      .mapValues(_.size)
      .toVector
      .sortBy(-_._2))
    val mostFrequent = sortedByFreq.head

    mostFrequent._2 should be (3 +- 2)
  }

  behavior of "ZipfianPicker for strings"

  "it" should "be possible to iterate through the list of strings" in {
    val picker = ZipfianPicker[String]("file:///" + getClass.getResource("/strings.txt").getPath, seed)

    var counter = 0
    for (elem <- picker) {
      counter += 1
      elem should not be empty
    }
    counter should be (20)
    picker.hasNext should be (false)
  }

  "it" should "be possible reset the iterator with other seed" in {
    val picker1 = ZipfianPicker[String]("file:///" + getClass.getResource("/strings.txt").getPath, seed)
    val picker2 = ZipfianPicker[String]("file:///" + getClass.getResource("/strings.txt").getPath, 24)

    val v1 = picker1.toVector
    picker1.reset(24)
    val v2 = picker1.toVector
    val v3 = picker2.toVector

    v1 should not equal(v2)
    v1 should not equal(v3)
    v2 should equal(v3)
  }

  "it" should "be possible use the ZipfianPicker for a remote file" in {
    val url = "https://raw.githubusercontent.com/radanalyticsio/equoid-data-publisher/master/data/LiquorNames.txt"
    val picker = ZipfianPicker[String](url)

    val (first, second) = (picker.next, picker.next)

    first should not be empty
    second should not be empty
  }

  "it" should "be possible to prepend a new frequent item" in {
    val picker = ZipfianPicker[String]("file:///" + getClass.getResource("/strings.txt").getPath, seed)
    val frequent = "frequent"
    // put the "frequent" item on the first 5 places in the sequence
    val newPicker = picker.prepend(frequent).prepend(frequent).prepend(frequent).prepend(frequent).prepend(frequent)

    val sortedByFreq = ((for (elem <- newPicker) yield elem)
      .toVector
      .groupBy(identity)
      .mapValues(_.size)
      .toVector
      .sortBy(-_._2))
    val mostFrequent = sortedByFreq.head

    mostFrequent._1 should equal(frequent)
  }

}
