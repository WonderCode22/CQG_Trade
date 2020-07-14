import os
import json
import threading
import pandas as pd
from numpyencoder import NumpyEncoder
from apscheduler.schedulers.blocking import BlockingScheduler
from CELEnvironment import CELEnvironment, Trace, pythoncom, logger
from SignalCheck import SignalCheck
from SubmitOrder import SubmitOrder
from Configuration import *

class OrderThread(threading.Thread):
    def __init__(self, properties, credentials):
        threading.Thread.__init__(self)
        self.properties = properties
        self.credentials = credentials

    def run(self):
        sched = BlockingScheduler()

        for i in range(len(self.properties['signal_check'])):
            check_data = self.properties['signal_check'][i]
            sched.add_job(signal_check_runner,
                          'date',
                          run_date=check_data['time'],
                          timezone='UTC',
                          args=[SignalCheck,
                                self.properties['order_type'],
                                check_data]
            )

        sched.add_job(trade_runner,
                      'date',
                      run_date=self.properties['trade_time'],
                      timezone='UTC',
                      args=[SubmitOrder, self.properties, self.credentials]
                      )
        sched.start()

def signal_check_runner(signalCheckCls, trigger, conditionProperties):
    pythoncom.CoInitialize()
    signalCelEnvironment = CELEnvironment()
    try:
        signalCheckInstance = signalCelEnvironment.Init(signalCheckCls, None)
        if not signalCelEnvironment.errorHappened:
            signalCheckInstance.Init(signalCelEnvironment,
                                     symbol=conditionProperties['symbol'],
                                     signal_level=conditionProperties['cond'],
                                     operator=conditionProperties['operator'],
                                     trigger=trigger)
            result, trade_price = signalCheckInstance.Start()
            conditionProperties['price'] = trade_price
            if result:
                conditionProperties['result'] = 1
            else:
                conditionProperties['result'] = 0
    except Exception as e:
        conditionProperties['price'] = None
        conditionProperties['result'] = 0
        Trace("Exception: {}".format(str(e)))
    finally:
        signalCelEnvironment.Shutdown()
    save_json_file(trade_properties)

def trade_runner(submitOrderCls, tradeProperties, credentials):
    signalCheckResult = 1
    for ele in tradeProperties['signal_check']:
        signalCheckResult *= ele['result']

    if signalCheckResult == 1 and tradeProperties['order_type'] != 0:
        orderCelEnvironment = CELEnvironment()
        try:
            submitOrderInstance = orderCelEnvironment.Init(submitOrderCls, None)
            if not orderCelEnvironment.errorHappened:
                submitOrderInstance.Init(orderCelEnvironment,
                                         symbol=tradeProperties['symbol'],
                                         order_count=tradeProperties['order_amount'],
                                         execution_time=tradeProperties['execution_window_time'],
                                         trigger=tradeProperties['order_type'],
                                         tick=tradeProperties['tick'],
                                         username=credentials['cqg_username'],
                                         password=credentials['cqg_password'])
            failedOrderCount = submitOrderInstance.Start()

            if tradeProperties['order_type'] == 1:
                orderType = 'Buy'
            elif tradeProperties['order_type'] == 0:
                orderType = 'Sell'

            logger.error(" Instrument's {} orders out of {} {} orders are remainning".format(failedOrderCount,
                                                                                             tradeProperties['order_amount'],
                                                                                             orderType))

        except Exception as e:
            logger.error("Exception: {}".format(str(e)))
            Trace("Exception: {}".format(str(e)))
        finally:
            orderCelEnvironment.Shutdown()
    else:
        logger.error("{} instrument's condition doesn't meet".format(tradeProperties['symbol']))
        Trace("{} instrument's condition doesn't meet".format(tradeProperties['symbol']))

def save_json_file(data, encoder=None):
    with open('demo_json.json', 'w') as f:
        if encoder:
            json.dump({'data': data}, f, indent=4, cls=encoder)
        else:
            json.dump({'data': data}, f, indent=4)


if __name__ == '__main__':
    credential = {
        'cqg_username': os.getenv('CQG_USRENAME', 'ATangSim'),
        'cqg_password': os.getenv('CQG_PASSWORD', 'pass')
    }

    # Read CSV, convert dataframe to Dict and write to JSON file
    if READ_CSV:
        controls_data = []
        control_df = pd.read_csv('sample_input.csv')
        control_df = control_df.fillna("")

        for i in range(len(control_df.index)):
            control_dict = {}
            control_dict = control_df.loc[i, 'strategy':'tick'].to_dict()

            signal_check_list = []
            for j in range(3):
                from_column = f'sc{j + 1}_symbol'
                to_column = f'sc{j + 1}_result'

                if control_df.loc[i, from_column] != '':
                    signal_check_block = control_df.loc[i, from_column:to_column].to_dict()
                    updated_check_block = {}

                    for key, value in signal_check_block.items():
                        new_key = key.split("_")[1]
                        updated_check_block[new_key] = value
                    signal_check_list.append(updated_check_block)
            control_dict['signal_check'] = signal_check_list
            controls_data.append(control_dict)

        # Write csv data to JSON file
        save_json_file(controls_data, NumpyEncoder)

    f = open('demo_json.json')
    trade_properties = json.load(f)['data']

    for i in range(len(trade_properties)):
        thread = OrderThread(trade_properties[i], credential)
        thread.start()
