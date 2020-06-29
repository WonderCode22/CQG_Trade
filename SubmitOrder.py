# The sample demonstrates how to place an order with Good Till Date property set.
import time
import threading
import win32com.client
from win32com.client import constants
from Configuration import *
from CELEnvironment import Trace, AssertMessage
from SignalCheck import StrategyInit


class SubmitOrder(StrategyInit):
    def __init__(self):
        self.account = None
        self.instrument = None
        self.eventInstrumentIsReady = threading.Event()
        self.eventOrderPlaced = threading.Event()
        self.eventOrderCancelled = threading.Event()

    def Init(self, celEnvironment, **kwargs):
        self.leftOrderCount = kwargs.get('order_count', 1)
        self.executionTime = kwargs.get('execution_time', 1)
        self.tick = kwargs.get('tick', 1)
        self.tradeTrigger = kwargs.get('trigger', 1)
        self.account = kwargs.get('account', 1)
        
        super(SubmitOrder, self).Init(celEnvironment, **kwargs)

    def Start(self):
        Trace("{} instrument requesting...".format(self.symbol))
        self.celEnvironment.cqgCEL.NewInstrument(self.symbol)
        Trace("{} instrument waiting...".format(self.symbol))
        AssertMessage(self.eventInstrumentIsReady.wait(INSTRUMENT_SETUP_TIMEOUT), "Instrument resolution timeout!")

        dispatchedInstrument = win32com.client.Dispatch(self.instrument)

        if self.tradeTrigger == 1:
            bestBid = dispatchedInstrument.Bid
            AssertMessage(bestBid.IsValid, "Error! Can't set an order price due to invalid BBA")
            Trace("Best bid value is {}".format(bestBid.Price))
            orderPrice = bestBid.Price + self.tick
            Trace("Order price to submit is {}".format(orderPrice))
            orderSide = constants.osdBuy
        elif self.tradeTrigger == -1:
            bestAsk = dispatchedInstrument.Ask
            AssertMessage(bestAsk.IsValid, "Error! Can't set an order price due to invalid BBA")
            Trace("Best ask value is {}".format(bestAsk.Price))
            orderPrice = bestAsk.Price + self.tick
            Trace("Order price to submit is {}".format(orderPrice))
            orderSide = constants.osdSell

        Trace("Create limit order")
        minutes_passed = 0
        while minutes_passed < self.executionTime:
            if minutes_passed >= 1:
                Trace("Cancel unfilled {} orders.(Format)".format(self.leftOrderCount))
                self.celEnvironment.cqgCEL.CancelAllOrders(self.account, None, False, False, orderSide)

                Trace("Waiting for {} orders to be cancelled...(Format)".format(self.leftOrderCount))
                AssertMessage(self.eventOrderCancelled.wait(ORDER_CANCEL_TIMEOUT), "Order cancellation timeout!")

            order = win32com.client.Dispatch(self.celEnvironment.cqgCEL.CreateOrder(constants.otLimit, self.instrument,
                                                                                    self.account, self.leftOrderCount,
                                                                                    orderSide,
                                                                                    orderPrice))
            Trace("Place order")
            order.Place()
            Trace("Waiting for {} orders placing...".format(self.leftOrderCount))
            AssertMessage(self.eventOrderPlaced.wait(ORDER_PLACE_TIMEOUT), "Order placing timeout!")
            time.sleep(60)

            minutes_passed = minutes_passed + 1
            if self.leftOrderCount == 0:
                break

    def OnInstrumentResolved(self, symbol, instrument, cqgError):
        if cqgError:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("OnInstrumentResolved error: Error code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                                      dispatchedCQGError.Description))
            return
        self.instrument = instrument
        Trace("Instrument {} is resolved!".format(symbol))
        self.eventInstrumentIsReady.set()

    def OnOrderChanged(self, changeType, cqgOrder, oldProperties, cqgFill, cqgError):
        if cqgError is not None:
            dispatchedCQGError = win32com.client.Dispatch(cqgError)
            Trace("OnOrderChanged error: Code: {} Description: {}".format(dispatchedCQGError.Code,
                                                                          dispatchedCQGError.Description))
            return

        dispatchedOrder = win32com.client.Dispatch(cqgOrder)
        properties = win32com.client.Dispatch(dispatchedOrder.Properties)
        gwStatus = properties(constants.opGWStatus)
        quantity = properties(constants.opQuantity)
        instrument = properties(constants.opInstrumentName)
        limitPrice = properties(constants.opLimitPrice)

        if gwStatus.Value == constants.osFilled:
            filledQuantity = properties(constants.opFilledQuantity)
            self.leftOrderCount -= filledQuantity.Value
            Trace("{} orders are filled and {} orders are left".format(filledQuantity.Value, self.leftOrderCount))
        else:
            Trace("OnOrderChanged: change type: {}; GW status: {}; Quantity: {}; Instrument: {}; Price: {};"
                .format(changeType, gwStatus, quantity, instrument, limitPrice))

        if changeType != constants.ctChanged:
            return

        if gwStatus.Value == constants.osInOrderBook:
            Trace("Order is placed!")
            self.eventOrderPlaced.set()

        if gwStatus.Value == constants.osCanceled:
            Trace("Order is cancelled!")
            self.eventOrderCancelled.set()

    def OnAllOrdersCanceled(self, orderSide, gwAccountIds, instrumentNames):
        Trace("Cancelled instruments are {}".format(instrumentNames))
        self.eventOrderCancelled.set()
