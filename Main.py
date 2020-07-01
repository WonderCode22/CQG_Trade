import os
import json
import threading
from apscheduler.schedulers.blocking import BlockingScheduler
from CELEnvironment import CELEnvironment, Trace, pythoncom
from SignalCheck import SignalCheck
from SubmitOrder import SubmitOrder


class OrderThread(threading.Thread):
    def __init__(self, index, properties, credentials):
        threading.Thread.__init__(self)
        self.index = index
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
                                self.properties['symbol'],
                                self.properties['order_type'],
                                check_data]
            )

        sched.add_job(trade_runner,
                      'date',
                      run_date=self.properties['trade_time'],
                      timezone='UTC',
                      args=[SubmitOrder, self.index, self.credentials]
                      )
        sched.start()

def signal_check_runner(signalCheckCls, symbol, trigger, conditionProperties):
    pythoncom.CoInitialize()
    signalCelEnvironment = CELEnvironment()
    try:
        signalCheckInstance = signalCelEnvironment.Init(signalCheckCls, None)
        if not signalCelEnvironment.errorHappened:
            signalCheckInstance.Init(signalCelEnvironment,
                                     symbol=symbol,
                                     signal_level=conditionProperties['cond'],
                                     operator=conditionProperties['operator'],
                                     trigger=trigger)
            result, trade_price = signalCheckInstance.Start()
            conditionProperties['price'] = trade_price
            if result:
                conditionProperties['result'] = 1
            else:
                conditionProperties['result'] = 0
            save_json_file(trade_properties)
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        signalCelEnvironment.Shutdown()

def trade_runner(submitOrderCls, index, credentials):
    tradeProperty = trade_properties[index]
    Trace("==========================\n{}".format(tradeProperty))
    signalCheckResult = 1
    for ele in tradeProperty['signal_check']:
        signalCheckResult *= ele['result']

    if signalCheckResult == 1:
        orderCelEnvironment = CELEnvironment()
        try:
            submitOrderInstance = orderCelEnvironment.Init(submitOrderCls, None)
            if not orderCelEnvironment.errorHappened:
                submitOrderInstance.Init(orderCelEnvironment,
                                         symbol=tradeProperty['symbol'],
                                         order_count=tradeProperty['order_amount'],
                                         execution_time=tradeProperty['execution_window_time'],
                                         trigger=tradeProperty['order_type'],
                                         tick=tradeProperty['tick'],
                                         username=credentials['cqg_username'],
                                         password=credentials['cqg_password'])
            submitOrderInstance.Start()
        except Exception as e:
            Trace("Exception: {}".format(str(e)))
        finally:
            orderCelEnvironment.Shutdown()
    else:
        Trace("{} instrument's condition doesn't meet".format(tradeProperty['symbol']))

def save_json_file(data):
    with open('demo_json.json', 'w') as f:
        json.dump({'data': data}, f, indent=4)


if __name__ == '__main__':
    credential = {
        'cqg_username': os.getenv('CQG_USRENAME', 'ATangSim'),
        'cqg_password': os.getenv('CQG_PASSWORD', 'pass')
    }

    f = open('demo_json.json')
    trade_properties = json.load(f)['data']

    for idx in range(len(trade_properties)):
        thread = OrderThread(idx, trade_properties[idx], credential)
        thread.start()

    # control_df = pd.read_csv('demo_6_19_20.csv')  # Read in control
    # control_df['Time_Date'] = pd.to_datetime(control_df['Time_Date'])
    # control_df = control_df.dropna(axis=0, how='all')

    # New Dataframe, tickDiff must be the same as tickDiff in control file. This is to address existing positions tickDiff
    # tick_master_df = pd.read_csv('tick_diff_master.csv')

    # strategies = control_df['Strategy'].unique()
    # for i in range(len(strategies)):
    #     strategy_df = control_df.loc[control_df['Strategy'] == strategies[i]]
    #     tick_df = tick_master_df.loc[tick_master_df['Contract'] == strategy_df.iloc[0]['Contract']].groupby('Contract').mean()
    #     strategy_df = strategy_df.merge(tick_df, how='left', on='Contract')
    #
    #     thread = OrderThread(strategy_df)
    #     thread.start()
