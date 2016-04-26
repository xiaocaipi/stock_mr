package com.sourcelearning.le03

import org.apache.spark.SparkContext
import scala.util.control.ControlThrowable
import scala.util.control.NonFatal
import com.sourcelearning.le03.Logger

object Utils {
  /** Return the class name of the given object, removing all dollar signs */
  def getFormattedClassName(obj: AnyRef): String = {
    obj.getClass.getSimpleName.replace("$", "")
  }
  
  
  def tryOrStopSparkContext(sc: SparkContext)(block: => Unit) {
    try {
      block
    } catch {
      case e: ControlThrowable => throw e
      case t: Throwable =>
        val currentThreadName = Thread.currentThread().getName
        if (sc != null) {
          sc.stop()
        }
        if (!NonFatal(t)) {
          throw t
        }
    }
  }
  
  
}