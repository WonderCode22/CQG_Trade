import os
import time
import datetime
import threading
from apscheduler.schedulers.blocking import BlockingScheduler
from CELEnvironment import CELEnvironment, Trace, pythoncom
from SignalCheck import SignalCheck
from SubmitOrder import SubmitOrder
from Gateway import GatewayHandle

def trade_runner(signalCheckCls, submitOrderCls, timeDelta, cqgAccount, tradeProperties):
    pythoncom.CoInitialize()
    signalCelEnvironment = CELEnvironment()
    try:
        signalCheckInstance = signalCelEnvironment.Init(signalCheckCls, None)
        if not signalCelEnvironment.errorHappened:
            signalCheckInstance.Init(signalCelEnvironment,
                                     symbol=tradeProperties['instrument_name'],
                                     signal_level=tradeProperties['signal_level'],
                                     operator=tradeProperties['signal_direction'],
                                     trigger=tradeProperties['trade_trigger'],)
            result = signalCheckInstance.Start()
            print(result)
            if result:
                orderCelEnvironment = CELEnvironment()
                try:
                    time.sleep(timeDelta) # Wait until trade_time (signalTime + timeDelta = tradeTime)
                    submitOrderInstance = orderCelEnvironment.Init(submitOrderCls, None)
                    if not orderCelEnvironment.errorHappened:
                        submitOrderInstance.Init(orderCelEnvironment,
                                                 symbol=tradeProperties['instrument_name'],
                                                 order_count=tradeProperties['order_count'],
                                                 execution_time=tradeProperties['execution_window_time'],
                                                 trigger=tradeProperties['trade_trigger'],
                                                 tick=tradeProperties['tick'],
                                                 account=cqgAccount)
                    submitOrderInstance.Start()
                except Exception as e:
                    Trace("Exception: {}".format(str(e)))
                finally:
                    orderCelEnvironment.Shutdown()
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        signalCelEnvironment.Shutdown()


class OrderThread(threading.Thread):
    def __init__(self, tradeProperties, cqgAccount):
        threading.Thread.__init__(self)
        self.properties = tradeProperties
        self.account = cqgAccount

    def run(self):
        signalCheckTimeStamp = datetime.datetime.strptime(self.properties['signal_time'], '%H%M')
        tradeTimeStamp = datetime.datetime.strptime(self.properties['trade_time'], '%H%M')
        timeDelta = (tradeTimeStamp - signalCheckTimeStamp).seconds

        sched = BlockingScheduler()
        sched.add_job(trade_runner,
                      'cron',
                      hour=self.properties['signal_time'][0:2],
                      minute=self.properties['signal_time'][2:],
                      timezone='UTC',
                      args=[SignalCheck, SubmitOrder, timeDelta, self.account, self.properties])
        sched.start()


def trade_scheduler(tradeProperties):
    # Calculate time delta(seconds) between tradeTime and signalCheckTime
    signalCheckTimeStamp = datetime.datetime.strptime(tradeProperties['signal_time'], '%H%M')
    tradeTimeStamp = datetime.datetime.strptime(tradeProperties['trade_time'], '%H%M')
    timeDelta = (tradeTimeStamp - signalCheckTimeStamp).seconds

    sched = BlockingScheduler()
    sched.add_job(trade_runner,
                  'cron',
                  hour=tradeProperties['signal_time'][0:2],
                  minute=tradeProperties['signal_time'][2:],
                  timezone='UTC',
                  args=[SignalCheck, SubmitOrder, timeDelta, tradeProperties])
    sched.start()


if __name__ == '__main__':
    trade_properties1 = {
        "instrument_name": "F.US.ZUS", # instrument symbol
        "signal_level": 1, # Bid/Ask value to be compared to check if condition is true or false.
        "signal_direction": '> by 10%', # checking method
        "signal_time": "1118",  # trade checking time. %H%M
        "trade_time": "1118",  # trade time if condition is true. %H%M
        "order_count": 20,
        "execution_window_time": 5,
        "trade_trigger": 1,
        "tick": 1,
        "cqg_username": os.getenv('CQG_USRENAME', 'ATangSim'),
        "cqg_password": os.getenv('CQG_PASSWORD', 'pass'),
    }

    trade_properties2 = {
        "instrument_name": "F.US.ZUA",  # instrument symbol
        "signal_level": 1,  # Bid/Ask value to be compared to check if condition is true or false.
        "signal_direction": '> by 10%',  # checking method
        "signal_time": "1118",  # trade checking time. %H%M
        "trade_time": "1119",  # trade time if condition is true. %H%M
        "order_count": 10,
        "execution_window_time": 10,
        "trade_trigger": -1,
        "tick": 1
    }
    trade_properties = [trade_properties1, trade_properties2]

    cqg_username = os.getenv('CQG_USRENAME', 'ATangSim')
    cqg_password = os.getenv('CQG_PASSWORD', 'pass')

    gatewayCelEnvironment = CELEnvironment()
    try:
        gatewayHandleInstance = gatewayCelEnvironment.Init(GatewayHandle, None)
        if not gatewayCelEnvironment.errorHappened:
            gatewayHandleInstance.Init(gatewayCelEnvironment,
                                     username=cqg_username,
                                     password=cqg_password)
            cqg_account = gatewayHandleInstance.Open()

            for i in range(2):
                thread = OrderThread(trade_properties[i], cqg_account)
                thread.start()
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        gatewayCelEnvironment.Shutdown()

