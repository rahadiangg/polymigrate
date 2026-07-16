#!/usr/bin/env bash
# Spin up two single-node MongoDB replica sets (source rs0 :27018, dest rs1 :27019)
# reachable from the host, and seed the source. Change streams require a replica
# set, so single-node RS is the minimum that exercises live CDC migration.
#
# Port trick: run mongod on the SAME port inside and outside the container
# (--port N + -p N:N) and advertise localhost:N, so the node can reach itself
# internally AND the host can reach it. Standard workaround for host-accessible
# single-node RS.
set -uo pipefail

IMG="${MM_MONGO_IMG:-mongo:7.0}"
SRC_PORT=27018
DST_PORT=27019
SRC_RS=rs0
DST_RS=rs1

teardown() {
  docker rm -f mm-src mm-dst >/dev/null 2>&1
}

up() {
  teardown
  echo ">> pulling $IMG (if needed) and starting containers"
  docker run -d --name mm-src -p ${SRC_PORT}:${SRC_PORT} "$IMG" \
    mongod --replSet $SRC_RS --port ${SRC_PORT} --bind_ip_all >/dev/null
  docker run -d --name mm-dst -p ${DST_PORT}:${DST_PORT} "$IMG" \
    mongod --replSet $DST_RS --port ${DST_PORT} --bind_ip_all >/dev/null

  echo ">> waiting for mongod to accept connections"
  for c in "mm-src ${SRC_PORT}" "mm-dst ${DST_PORT}"; do
    set -- $c
    for i in $(seq 1 60); do
      if docker exec $1 mongosh --quiet --port $2 --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then
        echo "   $1 up"; break
      fi
      sleep 1
      [ $i -eq 60 ] && { echo "   $1 FAILED to start"; docker logs $1 | tail -20; return 1; }
    done
  done

  echo ">> initiating replica sets"
  docker exec mm-src mongosh --quiet --port ${SRC_PORT} --eval \
    "rs.initiate({_id:'${SRC_RS}',members:[{_id:0,host:'localhost:${SRC_PORT}'}]})" >/dev/null
  docker exec mm-dst mongosh --quiet --port ${DST_PORT} --eval \
    "rs.initiate({_id:'${DST_RS}',members:[{_id:0,host:'localhost:${DST_PORT}'}]})" >/dev/null

  echo ">> waiting for primaries"
  for c in "mm-src ${SRC_PORT}" "mm-dst ${DST_PORT}"; do
    set -- $c
    for i in $(seq 1 60); do
      if docker exec $1 mongosh --quiet --port $2 --eval 'db.hello().isWritablePrimary' 2>/dev/null | grep -q true; then
        echo "   $1 primary"; break
      fi
      sleep 1
      [ $i -eq 60 ] && { echo "   $1 no primary"; return 1; }
    done
  done
}

seed() {
  echo ">> seeding source (shop.orders, shop.users with indexes; analytics.events)"
  docker exec mm-src mongosh --quiet --port ${SRC_PORT} --eval '
    const d = db.getSiblingDB("shop");
    d.orders.drop(); d.users.drop();
    const orders = [];
    for (let i=0;i<5000;i++) orders.push({_id:i, sku:"sku-"+(i%50), qty:(i%9)+1, total: (i*1.25), ts:new Date(1700000000000+i*1000)});
    d.orders.insertMany(orders);
    const users = [];
    for (let i=0;i<1200;i++) users.push({_id:"u"+i, email:"user"+i+"@ex.com", tags:["t"+(i%7),"t"+(i%3)], profile:{age:18+(i%40), city:"c"+(i%20)}});
    d.users.insertMany(users);
    d.orders.createIndex({sku:1});
    d.orders.createIndex({ts:-1}, {name:"ts_desc"});
    d.users.createIndex({email:1}, {unique:true});
    d.users.createIndex({"profile.city":1}, {sparse:true});
    const a = db.getSiblingDB("analytics");
    a.events.drop();
    const ev=[]; for(let i=0;i<800;i++) ev.push({_id:i, kind:"click", path:"/p/"+(i%30), n:i});
    a.events.insertMany(ev);
    print("seeded: orders="+d.orders.countDocuments()+" users="+d.users.countDocuments()+" events="+a.events.countDocuments());
  '
}

# Generate live writes on the source (for CDC testing). Arg: number of ops.
writes() {
  local n="${1:-500}"
  docker exec mm-src mongosh --quiet --port ${SRC_PORT} --eval "
    const d = db.getSiblingDB('shop');
    for (let i=0;i<${n};i++) {
      const id = 100000+i;
      d.orders.insertOne({_id:id, sku:'live-'+(i%10), qty:1, total:i, ts:new Date()});
      if (i%5==0) d.orders.updateOne({_id: i}, {\$set:{qty: 99}});
      if (i%17==0) d.orders.deleteOne({_id: 50000+i});
    }
    print('applied ${n} live write-bursts');
  "
}

case "${1:-up}" in
  up) up && seed ;;
  seed) seed ;;
  writes) writes "${2:-500}" ;;
  down) teardown; echo "torn down" ;;
  *) echo "usage: $0 {up|seed|writes N|down}"; exit 1 ;;
esac
