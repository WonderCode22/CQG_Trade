
import threading
import win32com.client
from CELEnvironment import CELSinkBase, AssertMessage, Trace


class StrategyInit(CELSinkBase):
    def Init(self, celEnvironment, **kwargs):
        self.celEnvironment = celEnvironment
        self.symbol = kwargs.get('symbol', 'DD')


class SignalCheck(StrategyInit):
    def __init__(self):
        self.instrument = None
        self.eventInstrumentIsReady = threading.Event()

    def Init(self, celEnvironment, **kwargs):
        self.signalLevel = kwargs.get('signal_level', '')
        self.tradeTrigger = kwargs.get('trigger', '')
        self.signalDirection = kwargs.get('operator', '')

        super(SignalCheck, self).Init(celEnvironment, **kwargs)

    def Start(self):
        Trace("{} instrument requesting...".format(self.symbol))
        self.celEnvironment.cqgCEL.NewInstrument(self.symbol)
        Trace("{} instrument waiting...".format(self.symbol))
        AssertMessage(self.eventInstrumentIsReady.wait(10), "Instrument resolution timeout!")

        dispatchedInstrument = win32com.client.Dispatch(self.instrument)
        bestBid = dispatchedInstrument.Bid
        AssertMessage(bestBid.IsValid, "Error! Can't set an order price due to invalid BBA")
        Trace("Best bid price is {}".format(bestBid.Price))

        if self.tradeTrigger != 0 and self.SignalOperationCalc(bestBid.Price, self.signalDirection):
            return True

        return False

    def OnInstrumentResolved(self, symbol, instrument, cqgError):
        if cqgError:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("OnInstrumentResolved error: Error code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                                      dispatchedCQGError.Description))
            return

        self.instrument = instrument
        Trace("Instrument {} is resolved!".format(symbol))
        self.eventInstrumentIsReady.set()

    def SignalOperationCalc(self, signalBid: float, operator: str):
        if 'by' in operator:
            timesValue = float(operator.split(' by ')[1].replace('%', ''))
            operator = operator.split(' by ')[0]
            signalBid = signalBid * timesValue

        if operator == '>':
            if signalBid > self.signalLevel:
                return True
        elif operator == '>=':
            if signalBid >= self.signalLevel:
                return True
        elif operator == '<':
            if signalBid < self.signalLevel:
                return True
        elif operator == '<=':
            if signalBid <= self.signalLevel:
                return True

        return False