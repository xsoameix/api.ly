#!/usr/bin/env st-livescript
require! <[optimist pgrest async]>
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
      day   <- self.date_of_sitting sitting, plx
      bills <- misq.get_from_committe sitting, day
      self.save_motions_and_bills bills, plx, sitting
      done!

  date_of_sitting: (sitting, plx, cb) ->
    (err, found) <- plx.conn.query "select * from calendar where _calendar_sitting_id(calendar) = '#{sitting.id}';"
    rows = found.rows.filter -> it.summary == sitting.summary
    cb rows[0].date

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

new YSFactory conString
new CommitteeFactory conString
