package geomesa.core.util

import org.apache.commons.cli.{Option => Opt, Options, CommandLine}

package object shell {

  implicit class RichCL(cl: CommandLine) {

    def getOpt(o: Opt): Option[String] =
      cl.hasOption(o.getOpt) match {
        case true  => Some(cl.getOptionValue(o.getOpt))
        case false => None
      }
  }
}
