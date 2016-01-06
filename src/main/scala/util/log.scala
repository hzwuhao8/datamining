package util
import  org.slf4j.Logger
import org.slf4j.LoggerFactory
trait Log {
   lazy val  logname: String ="guidetodatamining"
   val log = LoggerFactory.getLogger(logname)
}