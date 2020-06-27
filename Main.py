import os
import time
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
from CELEnvironment import CELEnvironment, Trace, pythoncom
from SignalCheck import SignalCheck
from SubmitOrder import SubmitOrder


def check_signal_runner(signalCheckCls, submitOrderCls, secondsDelta):
    pythoncom.CoInitialize()
    signalCelEnvironment = CELEnvironment()
    try:
        signalCheckInstance = signalCelEnvironment.Init(signalCheckCls, None)
        if not signalCelEnvironment.errorHappened:
            signalCheckInstance.Init(signalCelEnvironment,
                                     symbol=symbol,
                                     signal_level=signalLevel,
                                     operator=signalDirection,
                                     trigger=tradeTrigger)
            result = signalCheckInstance.Start()
            print(result)
            if result:
                orderCelEnvironment = CELEnvironment()
                try:
                    time.sleep(secondsDelta)
                    submitOrderInstance = orderCelEnvironment.Init(submitOrderCls, None)
                    if not orderCelEnvironment.errorHappened:
                        submitOrderInstance.Init(orderCelEnvironment,
                                                 symbol=symbol,
                                                 order_count=orderAmount,
                                                 execution_time=executionTime,
                                                 trigger=tradeTrigger,
                                                 tick=tick,
                                                 username=cqg_username,
                                                 password=cqg_password)
                    submitOrderInstance.Start()
                except Exception as e:
                    Trace("Exception: {}".format(str(e)))
                finally:
                    orderCelEnvironment.Shutdown()
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        signalCelEnvironment.Shutdown()


if __name__ == '__main__':
    symbol = 'F.US.ZUC'  # instrument symbol
    signalLevel = 1  # Bid/Ask value to be compared to check if condition is true or false.
    signalDirection = '> by 10%'  # checking method
    tradeTrigger = 1  # 1: Buy(Ask), -1: Sell(Bid), 0: No trade
    signalCheckTime = "2027"  # trade checking time. %H%M
    tradeTime = "2027"  # trade time if condition is true. %H%M
    orderAmount = 10
    executionTime = 10  # total minutes to submit orders
    tick = 1
    cqg_username = os.getenv('CQG_USRENAME', 'ATangSim')
    cqg_password = os.getenv('CQG_PASSWORD', 'pass')

    # Calculate time delta(seconds) between tradeTime and signalCheckTime
    signalCheckTimeStamp = datetime.datetime.strptime(signalCheckTime, '%H%M')
    tradeTimeStamp = datetime.datetime.strptime(tradeTime, '%H%M')
    timeDelta = (tradeTimeStamp-signalCheckTimeStamp).seconds

    # sched = BlockingScheduler()
    # sched.add_job(check_signal_runner,
    #               'cron',
    #               hour=signalCheckTime[0:2],
    #               minute=signalCheckTime[2:],
    #               timezone='UTC',
    #               args=[SignalCheck, SubmitOrder, timeDelta])
    # sched.start()

    check_signal_runner(SignalCheck, SubmitOrder, timeDelta)
