package com.huatu.zac

import java.text.SimpleDateFormat
import java.util.Date

object mya {

  def main(args: Array[String]): Unit = {


    val format = new SimpleDateFormat("yyyyMMdd")
    val format2 = new SimpleDateFormat("yyyyw")

    val end = System.currentTimeMillis()

    val nWeek = format2.format(new Date(System.currentTimeMillis()))

    var year = Integer.parseInt(nWeek.substring(0, 4))
    var week =  Integer.parseInt(nWeek.substring(4, nWeek.length))


    var w = ""
    if (week == 1) {
      year = year - 1
      w = "52"
    } else if (week < 11 && week > 1) {
      w = 0 + "" + (week - 1)
    } else {
      w = (week - 1) + ""
    }

    val start = format2.parse(year + "" + w).getTime+24*60*60*1000



    println(year + "" + w)
    println(TimeUtils.convertDateStr2Date(year + "" + w,"yyyyw"))
    println(start)
  }
}
