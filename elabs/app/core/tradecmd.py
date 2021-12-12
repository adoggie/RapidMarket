#coding:utf-8
import cmd

from elabs.fundamental.utils.useful import input_params
from elabs.app.core.controller import Controller

class TradeCmd(cmd.Cmd):
  def __init__(self):
    cmd.Cmd.__init__(self)

  def do_help(self,*args):
    print("show pos      ---  show current position list. \n"
          "show tick  m  ---  show tick list. \n"
          "show order q  ---  show queuing order list. \n"
          "order send [ code price  quantity direction oc ] --- send order . \n"
          "      - price  0 or +/- shift \n"
          "      - quantity  int \n"
          "      - direction  buy or sell \n"
          "      - oc   close or open  \n"
          "order send RM201 0 1 buy open  -- \n"
          "order cancel xxx / all \n"
          "pos emit CF201 11 \n"
          "exit \n")

  def do_show(self,line):
    args = input_params(line,['pos'])
    if args:
      for k, p in self.positions.items():
        print(k, ' Long:',p[0].Position,' Profit:',p[0].PositionProfit, ', Short:', p[1].Position,' Profit:',p[1].PositionProfit , '| Position:', p[0].Position - p[1].Position)
      return

  # def do_order(self,line):
    args = input_params(line,['order','q'])
    if args:
      qorders = self.getQueueingOrders()
      for r in qorders:
        print( r.InstrumentID,',',Constants.DirectionType.reverseMap[r.Direction],
              ', OrderSysID:',r.OrderSysID,', order_id:',r.order_id , ', user_order_id:',r.user_order_id)
      return

    args = input_params(line,['tick'])
    if args:
      with self.lock:
        inss =args[1:]
        for t in self.ticks.values():
          if inss:
            for ins in inss:
              if t.InstrumentID.find(ins) != -1:
                print(t.InstrumentID, ' LastPrice:', t.LastPrice, ' Asks:', t.Asks, ' Bids:', t.Bids, ' AskVol:',
                      t.AskVols, ' BidVol:', t.BidVols,'HL:',t.UpperLimitPrice,t.LowerLimitPrice)
          else:
            print( t.InstrumentID, ' LastPrice:',t.LastPrice ,' Asks:',t.Asks,' Bids:',t.Bids,' AskVol:',t.AskVols,' BidVol:',t.BidVols,'HL:',t.UpperLimitPrice,t.LowerLimitPrice)

        if not inss:
          print('--'*30)
          print('Total : ', len(self.ticks.values()))
        return

  def do_order(self,line):
    args = input_params(line,['send'],5)
    if args:
      # code ,price, quantity,direction,oc
      # CF201 0 1 long/short  open/close
      """
      price:  0 -  lastprice , 
              -1 - (lastprice -1) 
              1  = (lastprice + 1)              
      """
      code,price,quant,direction,oc = args[:]
      price = float(price)
      quant = int(quant)

      tick = self.ticks.get(code)
      if not tick:
        print("Tick unreached,Please Retry later..")
        return
      insinfo = get_insrinfo(code)
      if not insinfo:
        print("Instrument not defined !")
        return
      price = price * insinfo['ticksize']  + tick.LastPrice

      if direction in ('long','buy'):
        direction = Constants.Buy
      elif direction in ('short','sell'):
        direction = Constants.Sell
      else:
        return
      if oc  == 'open':
        oc = Constants.Open
      elif oc == 'close':
        oc = Constants.Close
      else:
        return
      req = OrderRequest(code,price,quant,direction,oc)
      order_id = self.getTradeApi().sendOrder(req)
      print(order_id)
      return
    #-- end  send order --
    args = input_params(line, ['cancel'])
    if args:
      if args[-1] == 'all':
        # 全撤
        qorders = self.getQueueingOrders()
        for r in qorders:
          order_id = r.order_id
          ret = self.getTradeApi().cancelOrder(order_id)
          print(ret)
      else:
        order_id = ''.join( args[1:] )
        r = self.getTradeApi().cancelOrder(order_id)
        print(r)


  def do_exit(self,*args):
    Controller().close()
    print('bye bye!')

    return True
