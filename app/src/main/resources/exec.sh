#!/bin/bash
flink run -m yarn-cluster -yt /data/user_data/klapp/yhl/apps/lib  -yjm 1024 -ytm 1600 $(ls -l /data/user_data/klapp/yhl/apps/lib  |awk 'NR>1{print  "-C file:///data/user_data/klapp/yhl/apps/lib/"$9}' |tr  '\n'  '  ') -c com.pactera.yhl.apps.develop.MainDevelop  app-1.0-SNAPSHOT.jar -config_path ./lib/configuration.properties
