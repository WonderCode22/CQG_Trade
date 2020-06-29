import time
import threading
from CELEnvironment import Trace, AssertMessage
from CELEnvironment import CELEnvironment, CELSinkBase
import win32com.client


def QuoteType2String(quoteType):
    return {
        win32com.client.constants.qtAsk: "Best ask",
        win32com.client.constants.qtBid: "Best bid",
        win32com.client.constants.qtCohUndAsk: "Coherent underlying price for the best ask",
        win32com.client.constants.qtCohUndBid: "Coherent underlying price for the best bid",
        win32com.client.constants.qtDayHigh: "Current day high price",
        win32com.client.constants.qtDayLow: "Current day low price",
        win32com.client.constants.qtDayOpen: "Day open price",
        win32com.client.constants.qtImpliedAsk: "Implied best ask",
        win32com.client.constants.qtImpliedBid: "Implied best bid",
        win32com.client.constants.qtIndicativeOpen: "Indicative open",
        win32com.client.constants.qtMarker: "Marker price",
        win32com.client.constants.qtOutrightAsk: "Outright best ask",
        win32com.client.constants.qtOutrightBid: "Outright best bid",
        win32com.client.constants.qtSettlement: "Settlement price",
        win32com.client.constants.qtTodayMarker: "Marker price",
        win32com.client.constants.qtTrade: "Last trade price",
        win32com.client.constants.qtYesterdaySettlement: "Yesterday's settlement price"
    }[quoteType]


class RealTimeMarketDataReceiving(CELSinkBase):
    def __init__(self):
        self.eventDone = threading.Event()

    def Init(self, celEnvironment):
        self.celEnvironment = celEnvironment

    def Start(self):
        # for i in range(1, 4):
        #     symbol = "CLE?{}".format(i)
        symbol = 'F.US.FEPP'
        Trace("Request realtime market data for {}".format(symbol))
        self.celEnvironment.cqgCEL.NewInstrument(symbol)

        self.eventDone.wait(10)

    def OnDataError(self, cqgError, errorDescription):
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace(
                "OnDataError: Code: {} Description: {}".format(dispatchedCQGError.Code, dispatchedCQGError.Description))
        self.eventDone.set()

    def OnInstrumentResolved(self, symbol, cqgInstrument, cqgError):
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("OnInstrumentResolved error: Error code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                                      dispatchedCQGError.Description))
            self.eventDone.set()
            return

        instrument = win32com.client.Dispatch(cqgInstrument)
        Trace("OnInstrumentResolved: Symbol: {} Instrument full name: {}".format(symbol, instrument.FullName))
        instrument.DataSubscriptionLevel = win32com.client.constants.dsQuotesAndBBA
        Trace("OnInstrumentResolved: DataSubscriptionLevel is changed")
        bestBid = instrument.Ask
        AssertMessage(bestBid.IsValid, "Error! Can't set an order price due to invalid BBA")
        Trace("Best Bid for {} is {}".format(instrument.FullName, bestBid.Price))

    def OnInstrumentChanged(self, cqgInstrument, cqgQuotes, cqgInstrumentProperties):
        instrument = win32com.client.Dispatch(cqgInstrument)
        quotes = win32com.client.Dispatch(cqgQuotes)
        Trace("New quotes for {}".format(instrument.FullName))
        for quote in quotes:
            if (quote.IsValid):
                Trace("  Type: {} Price: {} Volume: {} Time: {}".format(QuoteType2String(quote.Type), quote.Price,
                                                                        quote.Volume, quote.Timestamp))


if __name__ == '__main__':
    symbols = ['F.US.OBX', 'F.US.TEL']

    trade_data = {}
    for symbol in symbols:
        trade_data[symbol] = []

    celEnvironment = CELEnvironment()
    try:
        sample = celEnvironment.Init(RealTimeMarketDataReceiving, None)
        if not celEnvironment.errorHappened:
            sample.Init(celEnvironment)
            sample.Start()
    except Exception as e:
        Trace("Exception: {}".format(str(e)))
    finally:
        celEnvironment.Shutdown()
