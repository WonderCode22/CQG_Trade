
import threading
import win32com.client
from Configuration import *
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
        self.signalLevel = kwargs.get('signal_level')
        self.tradeTrigger = kwargs.get('trigger')
        self.signalDirection = kwargs.get('operator')

        super(SignalCheck, self).Init(celEnvironment, **kwargs)

    def Start(self):
        Trace("{} instrument requesting...".format(self.symbol))
        self.celEnvironment.cqgCEL.NewInstrument(self.symbol)
        Trace("{} instrument waiting...".format(self.symbol))
        AssertMessage(self.eventInstrumentIsReady.wait(INSTRUMENT_SETUP_TIMEOUT), "Instrument resolution timeout!")

        dispatchedInstrument = win32com.client.Dispatch(self.instrument)

        if self.tradeTrigger == 1:
            bestTrade = dispatchedInstrument.Bid
        elif self.tradeTrigger == -1:
            bestTrade = dispatchedInstrument.Ask
        AssertMessage(bestTrade.IsValid, "Error! Can't set an order price due to invalid BBA")
        Trace("{}'s best price is {}".format(self.symbol, bestTrade.Price))

        if self.tradeTrigger != 0 and self.SignalOperationCalc(bestTrade.Price, self.signalDirection):
            return True, bestTrade.Price

        return False, bestTrade.Price

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
            if self.signalLevel > signalBid:
                return True
        elif operator == '>=':
            if self.signalLevel >= signalBid:
                return True
        elif operator == '<':
            if self.signalLevel < signalBid:
                return True
        elif operator == '<=':
            if self.signalLevel <= signalBid:
                return True

        return False