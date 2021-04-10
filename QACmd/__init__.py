import cmd
import subprocess

from QASU.save_binance import QA_SU_save_binance_1min
from QASU.save_okex import QA_SU_save_okex_1min, QA_SU_save_okex
from QAUtil.QALogs import QA_util_log_info

__version__ = '1.0410.001'

class CLI(cmd.Cmd):
    def __init__(self):
        cmd.Cmd.__init__(self)
        self.prompt = "QUANTAXIS> "  # 定义命令行提示符

    def do_shell(self, arg):
        print(">", arg)
        sub_cmd = subprocess.Popen(arg, shell=True, stdout=subprocess.PIPE)
        print(sub_cmd.communicate()[0])

    def do_version(self, arg):
        QA_util_log_info(__version__)

    def do_save(self, arg):
        print("arg=", arg)
        if arg == "":
            print("没有参数")
        else:
            arg = arg.split(" ")

            if len(arg) == 1 and arg[0] == "all":
                print("全部数据")
            elif len(arg) == 1 and arg[0] == "binance":
                QA_SU_save_binance_1min()
            elif len(arg) == 1 and arg[0] == "okex":
                QA_SU_save_okex_1min()
            elif len(arg) == 2 and arg[0] == "okex":
                if arg[1] == "all":
                    QA_SU_save_okex_1min()
                else:
                    frequency = arg[1]
                    QA_SU_save_okex(frequency)


def QA_cmd():
    cli = CLI()
    cli.cmdloop()