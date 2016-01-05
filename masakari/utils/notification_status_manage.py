#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright(c) 2015 Nippon Telegraph and Telephone Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import MySQLdb
import argparse
import subprocess


class notification_status_manage(object):

    def __init__(self):

        parser = argparse.ArgumentParser(prog='notification_status_manage.py', add_help=False)

        parser.add_argument('--mode', help='list')
        parser.add_argument('--db-user', help='mysql user name')
        parser.add_argument('--db-password', help='mysql user password')
        parser.add_argument('--db-host', help='mysql host name')

        args = parser.parse_args()

        if not self._command_input_information_check(parser,args):
            return

        msg = "notification status manage execution start"
        print msg

        try:
            sysout_sql = self._notification_status_list(args.db_user,
                                                        args.db_password,
                                                        args.db_host)

            if sysout_sql is not None:
                 subprocess.call(sysout_sql, shell=True)

        except:
            msg = "rnotification status manage execution failure"
            print msg

        finally:
            msg = "notification status manage execution end"
            print msg


    def _command_input_information_check(self,parser,args):

        result = True

        if args.mode != "list":
            result = False

        if (args.db_user is None
         or args.db_password is None
         or args.db_host is None):
            result = False

        #usage display
        if not result:
            parser.print_help()

        return result


    def _db_connect(self,
                    mysql_user_name,
                    mysql_user_password,
                    mysql_host_name):

        try:
            db = MySQLdb.connect(host=mysql_host_name,
                                 db='vm_ha',
                                 user=mysql_user_name,
                                 passwd=mysql_user_password,
                                 charset='utf8')
            return db

        except:
            msg = "db connection failed"
            print msg
            raise


    def _notification_status_list(self,
                                  mysql_user_name,
                                  mysql_user_password,
                                  mysql_host_name):

        db = self._db_connect(args.db_user,
                              args.db_password,
                              args.db_host)

        # Execute SQL
        cursor = db.cursor(MySQLdb.cursors.DictCursor)

        sql = ("SELECT * FROM notification_list "
               "WHERE deleted = 0 "
               "AND (progress = 0 OR progress = 3)")

        try:
            row_cnt = cursor.execute(sql)
            if row_cnt == 0:
                msg = "none notification_list"
                print msg
                return None

            else:
                sql = ("mysql --host=%s --database=vm_ha "
                       "--user=%s --password=%s "
                       "-e\"SELECT "
                       "create_at,"
                       "update_at,"
                       "notification_id,"
                       "notification_type,"
                       "notification_regionID,"
                       "notification_hostname,"
                       "notification_uuid,"
                       "notification_time,"
                       "notification_eventID,"
                       "notification_eventType,"
                       "notification_detail,"
                       "notification_startTime,"
                       "notification_endTime,"
                       "notification_tzname,"
                       "notification_daylight,"
                       "notification_cluster_port,"
                       "progress,"
                       "recover_by "
                       "FROM notification_list "
                       "WHERE deleted = 0 "
                       "AND (progress = 0 OR progress = 3)\";"
                       ) % (mysql_host_name,
                            mysql_user_name,
                            mysql_user_password)

                return sql

        except:
            msg = "notification_list select failed"
            print msg
            raise

        finally:
            db.commit()
            db.close()


if __name__ == '__main__':
    notification_status_manage()


##########################################################################################
#
#(command)
#
#[python notification_status_manage.py --mode list --db-user root --db-password openstack --db-host localhost]
#
##########################################################################################

