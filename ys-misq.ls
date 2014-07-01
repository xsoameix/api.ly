#!/usr/bin/env st-livescript
require! <[optimist pgrest async cheerio request fs zhutil]>
{consume-events} = require \./lib/pgq
twly = require \./lib

{ queue="ldqueue", consumer="ys-misq", dry, flush, dir="/tmp/misq"} = optimist.argv

throw "queue and consumer required" unless queue and consumer

conString = optimist.argv.db ? process.env.PGDATABASE
conString = "localhost/#conString" unless conString is // / //
conString = "tcp://#conString"     unless conString is // :/ //

console.log "ys-misq listening"

{misq} = require \twlyparser

class MotionAndBillFactory

  (con_string) ->
    @consume_events con_string

  consume_events: (con_string) ->
    self = this;
    plx <- pgrest.new conString, {+client}
    batch, events, cb <- consume-events plx, {queue, consumer, table: 'public.sittings', interval: 200ms, dry: dry ? flush}
    funcs =
      for event in events
        if (self.insert_event_happen_p(event.ev_type) and
            self.handle_this_sitting_p(event.ev_data))
          self.create_motions_and_bills(plx, event.ev_data, batch, event.ev_id)
    return cb true if funcs.length == 0
    err, res <- async.series funcs
    cb true

  insert_event_happen_p: (ev_type) ->
    ev_type is 'I:id'

  # should implement by subclass
  handle_this_data_p: (sitting) ->
  create_motions_and_bills: (plx, sitting, batch, ev_id) ->

# 院會會議
class YSFactory extends MotionAndBillFactory

  handle_this_sitting_p: (sitting) ->
    # 如果不是委員會會議，那就是院會會議了
    !sitting.committee

  create_motions_and_bills: (plx, sitting, batch, ev_id) ->
    self = this;
    (done) ->
      sitting_id = sitting.id
      data <- misq.get sitting, {dir, +agenda-only}
      funcs   = self.announcements plx, data, sitting_id
      funcs ++= self.discussions plx, data, sitting_id
      err, res <- async.series funcs
      if err
        if err.data isnt /Status is a duplicate/
          console.log \== err
          return plx.query "select pgq.event_retry($1, $2, $3::int)" [batch, ev_id, 3], -> done!
      done!

  announcements: (plx, data, sitting_id) ->
    for a in data.announcement => let a
      (done) ->
        res <- plx.upsert {
          collection: \bills
          q: {bill_id: a.id}
          $: $set: {a.summary, proposed_by: a.proposer}
        }, _, -> console.log \err it
        res <- plx.upsert {
          collection: \motions
          q: {sitting_id, bill_id: a.id}
          $: $set:
            motion_class: \announcement
            agenda_item: a.agendaItem
            subitem: a.subItem
        }, _, -> console.log \err it
        console.log a.id, res
        done!

  discussions: (plx, data, sitting_id) ->
    for d in data.discussion => let d
      (done) ->
        res <- plx.upsert {
          collection: \bills
          q: {bill_id: d.id}
          $: $set: {d.summary, proposed_by: d.proposer}
        }, _, -> console.log \err it
        res <- plx.upsert {
          collection: \motions
          q: {sitting_id, bill_id: d.id}
          $: $set:
            motion_class: \discussion
            agenda_item: d.agendaItem
            subitem: d.subItem
        }, _, -> console.log \err it
        console.log d.id, res
        done!

# 委員會會議
class CommitteeFactory extends MotionAndBillFactory

  handle_this_sitting_p: (sitting) ->
    sitting.committee

  create_motions_and_bills: (plx, sitting, batch, ev_id) ->
    self = this;
    (done) ->
      page <- self.page_of_js_args sitting, plx
      rows  = self.rows_in_page page
      args  = self.js_args_in_rows rows, sitting
      page <- self.page_of_bills args
      bills = self.bills_in_page page
      bills = self.bills_with_cascade bills, sitting
      self.save_motions_and_bills bills, plx, sitting
      done!

  page_of_js_args: (sitting, plx, cb) ->
    self  = this
    day  <- @date_of_sitting sitting, plx
    year  = day.getYear! + 1900 - 1911
    month = self.rjust day.getMonth! + 1
    date  = self.rjust day.getDate!
    day   = "#year/#month/#date"
    uri   = 'http://misq.ly.gov.tw/MISQ/IQuery/misq5000QueryMeeting.action'
    err, res, body <- request do
      method: \POST
      uri: uri
      headers: do
        Origin: 'http://misq.ly.gov.tw'
        Referer: uri
        User-Agent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2078.0 Safari/537.36'
      form: do
        term:          self.rjust sitting.ad
        sessionPeriod: self.rjust sitting.session
        meetingDateS:  day
        meetingDateE:  day
        queryCondition: <[ 0703 2100 2300 2400 4000 4100 4200 4300 4500 ]>
    throw that if err
    page = cheerio.load body
    cb page

  date_of_sitting: (sitting, plx, cb) ->
    (err, found) <- plx.conn.query "select * from calendar where _calendar_sitting_id(calendar) = '#{sitting.id}';"
    for row in found.rows
      if row.summary == sitting.summary
        cb row.date
        break

  rows_in_page: (page) ->
    table = page '#queryListForm table'
    rows = @rows_of_table table, (col, header) ->
      if header == '會議名稱含事由'
        link = col.find '> a' .first!
        html = link.html! - /^\s*|\s*$/gm
        name = html.match /^.+(?=<br)/ .0
        jscode = link.attr 'onclick'
        args = jscode.match /[\d\/]+/g
        content = {name: name, args: args}
      else
        content = col.text! - /^\s*|\s*$/gm
    rows

  rows_of_table: (table, cb) ->
    rows = []
    headers = table.find 'th' .map -> @text!
    table.find '> tr' .map ->
      hash = {}
      cols = @find '> td'
      return if cols.length == 0
      cols.map (i) ->
        header = headers[i]
        content = cb this, header
        hash[header] = content
      rows.push hash
    rows

  js_args_in_rows: (rows, sitting) ->
    args = []
    for row in rows
      content = row['會議名稱含事由']
      if content.name == sitting.name
        args = content.args
    args

  rjust: (object) ->
    string = new AugmentedString object
    string.rjust 2, '0'

  page_of_bills: ([meetingNo, meetingTime, departmentCode], cb) ->
    uri = "http://misq.ly.gov.tw/MISQ/IQuery/misq5000QueryMeetingDetail.action"
    err, res, body <- request do
      method: \POST
      uri: uri
      headers: do
        Origin: 'http://misq.ly.gov.tw'
        Referer: uri
        User-Agent: 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.2078.0 Safari/537.36'
      form: {meetingNo, meetingTime, departmentCode}
    page = cheerio.load body
    cb page

  bills_in_page: (page) ->
    self = this
    bills = []
    page '#queryForm table tr' .each ->
      header = @find '> th' .map -> @text!
      return if header.length != 1
      header = header.0 - /：/
      content = @find '> td'
      if header == \關係文書
        links = content.find 'a[onclick]'
        bills := links.map -> self.bill_in_link this
    bills

  bill_in_link: (link) ->
    name = link.text! - /^\s*|\s*$/gm
    name = name.match /「.+」/ .0
    id = link.attr \onclick .match /\d+/ .0
    {name, id}

  bills_with_cascade: (bills, sitting) ->
    self = this
    bills = bills.map ->
      map = self.map_bill_to_cascade sitting
      [it, map[it.name]]
    bills = bills.filter (bill, cascade) -> cascade
    bills.map (bill, cascade) ->
      [...levels, summary] = cascade
      levels = levels.map -> zhutil.parseZHNumber it
      [level_1st, level_2nd] = levels
      {level_1st, level_2nd, summary, bill.id}

  map_bill_to_cascade: (sitting) ->
    return that if @map
    @map = {}
    lines = sitting.summary.split "\n"
    for line in lines
      switch
      case line.match @level_1st_reg!
        level_1st = that.1
        level_2nd = '一'
        summary   = that.2
      case line.match @level_2nd_reg!
        level_2nd = that.1
        summary   = that.2
      if line.match /「.+」/
        @map[that.0] = [level_1st, level_2nd, summary]
    @map

  level_1st_reg: ->
    return that if @lv_1st_reg
    zhnumber = <[○ 一 二 三 四 五 六 七 八 九 十]>
    zhnumber = (<[千 百 零]> ++ zhnumber) * '|'
    @lv_1st_reg = new RegExp "^\s*((?:#zhnumber)+)、(.*)$"

  level_2nd_reg: ->
    return that if @lv_2nd_reg
    zhnumber = <[○ 一 二 三 四 五 六 七 八 九 十]>
    zhnumber = zhnumber * '|'
    @lv_2nd_reg = new RegExp "^\s*（((?:#zhnumber)+)）(.*)$"

  save_motions_and_bills: (bills, plx, sitting) ->
    sitting_id = sitting.id
    funcs = bills.map (bill) ->
      (done) ->
        res <- plx.upsert {
          collection: \bills
          q: {bill_id: bill.id}
          $: $set: {bill.summary}
        }, _, -> console.log \err it
        res <- plx.upsert {
          collection: \motions
          q: {sitting_id, bill_id: bill.id}
          $: $set:
            motion_class: \discussion
            agenda_item: bill.level_1st
            subitem: bill.level_2nd
        }, _, -> console.log \err it
        console.log bill.id, res
        done!
    err, res <- async.series funcs
    throw that if err

class AugmentedString

  (@string) ->

  # "hello".rjust(4)            #=> "hello"
  # "hello".rjust(20)           #=> "               hello"
  # "hello".rjust(20, '1234')   #=> "123412341234123hello"
  rjust: (width, padding = ' ') ->
    len = width - @string.length
    if len > 0
      times  = len / padding.length
      remain = len % padding.length
      string = new AugmentedString padding
      string = string.repeat times
      tail   = padding.slice 0, remain
      string = string.concat tail
      string.concat @string
    else
      @string

  # "Ho! ".repeat(3)  #=> "Ho! Ho! Ho! "
  # "Ho! ".repeat(0)  #=> ""
  repeat: (times) ->
    clone = ''
    for i to times - 1
      clone = clone.concat @string
    clone

new YSFactory conString
new CommitteeFactory conString
