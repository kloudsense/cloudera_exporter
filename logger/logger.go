/*
 *
 * title           :collector/host.go
 * description     :Submodule Collector for the Cluster hosts metrics
 * author		       :Alejandro Villegas
 * date            :2019/02/04
 * version         :1.0
 *
 */

package kbdi_logger




/* 
 * Dependencies
 */
import (
  // Go Default libraries
  "io"
  "io/ioutil"
  "log"
  "fmt"
  "runtime"
  "path"
)




var (
    Ok        *log.Logger
    Info      *log.Logger
    Warning   *log.Logger
    Error     *log.Logger
    Debug     *log.Logger
    Log_level int
)




func Init(okHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer, debugHandle io.Writer, log_level int) {
    head :=       "\033[37m[\033[35mKBDI\033[37m]\033[0m"
    ok_head :=    "\033[37m[\033[32m OK \033[37m]\033[0m"
    info_head :=  "\033[37m[\033[34mINFO\033[37m]\033[0m"
    warn_head :=  "\033[37m[\033[33mWARN\033[37m]\033[0m"
    error_head := "\033[37m[\033[31mERROR\033[37m]\033[0m"
    debug_head := "\033[37m[\033[36mDEBUG\033[37m]\033[0m"
    Log_level = log_level

    Ok = log.New(okHandle,
      fmt.Sprintf("%s  %s ", ok_head, head),
      log.Ldate|log.Ltime)

    Info = log.New(infoHandle,
      fmt.Sprintf("%s  %s ", info_head, head),
      log.Ldate|log.Ltime)

    Warning = log.New(warningHandle,
      fmt.Sprintf("%s  %s ", warn_head, head),
      log.Ldate|log.Ltime)

    Error = log.New(errorHandle,
      fmt.Sprintf("%s %s ", error_head, head),
      log.Ldate|log.Ltime)

    if Log_level == 1 {
      Debug = log.New(debugHandle,
        fmt.Sprintf("%s %s ", debug_head, head),
        log.Ldate|log.Ltime)
      Warn_msg("Enabled Debug logger Mode")
    } else if Log_level == 0 {
      Debug = log.New(ioutil.Discard,
        fmt.Sprintf("%s %s ", debug_head, head),
        log.Ldate|log.Ltime)
      Debug_msg("Disabled Debug logger Mode")
    }
}


func format_msg (msg ...interface{}) string{
  return fmt.Sprintf(msg[0].(string), msg[1:]...)
}

func Ok_msg (msg ...interface{}) {
  Ok.Printf("%s", format_msg(msg...))
}

func Info_msg (msg ...interface{}) {
  Info.Printf("%s", format_msg(msg...))
}

func Warn_msg (msg ...interface{}) {
  Warning.Printf("%s", format_msg(msg...))
}

func Err_msg (msg ...interface{}){
  _, fileName, fileLine, _ := runtime.Caller(1)
  Error.Printf("%s:%d:  %s", path.Base(fileName), fileLine, format_msg(msg...))
}

func Debug_msg (msg ...interface{}){
  _, fileName, fileLine, _ := runtime.Caller(1)
  Debug.Printf("%s:%d:  %s", path.Base(fileName), fileLine, format_msg(msg...))
}
