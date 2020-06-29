# The sample demonstrates how to place an order with Good Till Date property set.
import time
import threading
import win32com.client
from win32com.client import constants
from Configuration import *
from CELEnvironment import Trace, AssertMessage
from SignalCheck import StrategyInit


class GatewayHandle(StrategyInit):
    def __init__(self):
        self.account = None
        self.eventGatewayIsUp = threading.Event()
        self.eventGatewayIsDown = threading.Event()
        self.eventAccountIsReady = threading.Event()

    def Init(self, celEnvironment, **kwargs):
        self.username = kwargs.get('username', '')
        self.password = kwargs.get('password', '')

        super(GatewayHandle, self).Init(celEnvironment, **kwargs)

    def Open(self):
        Trace("Connecting to GW")
        self.celEnvironment.cqgCEL.GWLogon(self.username, self.password)

        Trace("Waiting for GW connection...")
        AssertMessage(self.eventGatewayIsUp.wait(GATEWAYUP_TIMEOUT), "GW connection timeout!")

        self.celEnvironment.cqgCEL.AccountSubscriptionLevel = constants.aslAccountUpdatesAndOrders
        Trace("Waiting for accounts coming...")
        AssertMessage(self.eventAccountIsReady.wait(ACCOUNT_LOGIN_TIMEOUT), "Accounts coming timeout!")

        Trace("Select the first account")
        accounts = win32com.client.Dispatch(self.celEnvironment.cqgCEL.Accounts)
        self.account = win32com.client.Dispatch(accounts.ItemByIndex(0))

    def Close(self):
        Trace("Logoff from GW")
        self.eventGatewayIsDown.clear()
        self.celEnvironment.cqgCEL.GWLogoff()
        AssertMessage(self.eventGatewayIsDown.wait(GATEWAYDOWN_TIMEOUT), "GW disconnection timeout!")

        Trace("Done!")

    def OnGWConnectionStatusChanged(self, connectionStatus):
        if connectionStatus == constants.csConnectionUp:
            Trace("GW connection is UP!")
            self.eventGatewayIsUp.set()
        if connectionStatus == constants.csConnectionDown:
            Trace("GW connection is DOWN!")
            self.eventGatewayIsDown.set()

    def OnAccountChanged(self, change, account, position):
        if change != constants.actAccountsReloaded:
            return

        Trace("Accounts are ready!")
        self.eventAccountIsReady.set()
