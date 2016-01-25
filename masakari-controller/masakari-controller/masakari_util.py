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

"""
Management module of utility classes for VM recovery control
"""

import ConfigParser
import datetime
import json
import logging
import MySQLdb
import os
import paramiko
import re
import masakari_config as config
import socket
import subprocess
import sys
import syslog
import traceback


class RecoveryControllerUtilDb(object):

    """
    DB-related utility classes for VM recovery control
    """

    def __init__(self, config_object):
        self.rc_config = config_object
        self.rc_util = RecoveryControllerUtil(self.rc_config)
        self.rc_util_ap = RecoveryControllerUtilApi(self.rc_config)

    def insert_vm_list_db(self, notification_id, notification_uuid, retry_cnt):
        """
        VM list table registration
        :param :notification_id: Notification ID
                (used as search criteria for notification list table)
        :param :notification_uuid:VM of uuid
                (used as the registered contents of the VM list table)
        :param :retry_cnt:Retry count
                (used as the registered contents of the VM list table)
        """

        try:
            conf_db_dic = self.rc_config.get_value('db')
            conf_log_dic = self.rc_config.get_value('log')
            # Connect db
            db = MySQLdb.connect(host=conf_db_dic.get("host"),
                                 db=conf_db_dic.get("name"),
                                 user=conf_db_dic.get("user"),
                                 passwd=conf_db_dic.get("passwd"),
                                 charset=conf_db_dic.get("charset"))

            log_level = conf_log_dic.get("log_level")

            # Execute SQL
            cursor = db.cursor(MySQLdb.cursors.DictCursor)

            sql = "SELECT recover_to, recover_by FROM notification_list " \
                  "WHERE notification_id = '%s'" % (notification_id)
            cursor.execute(sql)

            result = cursor.fetchone()

            # rowcount is always '1' because notification_id is unique
            notification_recover_to = result.get('recover_to')
            notification_recover_by = result.get('recover_by')

            sql = ("INSERT INTO vm_list ( create_at, "
                   "deleted, "
                   "uuid, "
                   "progress, "
                   "retry_cnt, "
                   "notification_id, "
                   "recover_to, "
                   "recover_by ) "
                   "VALUES ( '%s', %s, '%s', %s, %s, '%s', '%s', %s ) "
                   % (datetime.datetime.now(),
                      "0",
                      notification_uuid,
                      "0",
                      str(retry_cnt),
                      notification_id,
                      notification_recover_to,
                      str(notification_recover_by)))

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0001",
                                      syslog.LOG_INFO)
            self.rc_util.syslogout("SQL=" + sql, syslog.LOG_INFO)

            cursor.execute(sql)
            db.commit()

            # db connection close
            cursor.close()
            db.close()

        except KeyError:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0002",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : KeyError in insert_vm_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise KeyError

        except MySQLdb.Error:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0003",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : MySQLdb.Error in insert_vm_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise MySQLdb.Error

        except:
            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0004",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)

            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : Exception in insert_vm_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

    def insert_notification_list_db(self, jsonData, recover_by):
        """
           Insert into notification_list DB from notification JSON.
           :param :jsonData: notifocation json data.
           :param :recover_by:node recover(0)/VM recover(1)/process error(2)
           :return :ret_dic:and return the information that was registered to
                       notification_list table in the dictionary type

        """

        # NOTE: Connection args will come from configurations.
        conf_db_dic = self.rc_config.get_value('db')
        con_args = {}
        con_args['host'] = conf_db_dic.get("host")
        con_args['db'] = conf_db_dic.get("name")
        con_args['user'] = conf_db_dic.get("user")
        con_args['passwd'] = conf_db_dic.get("passwd")
        con_args['charset'] = conf_db_dic.get("charset")

        columns = " (create_at, update_at, delete_at, deleted, " \
            " notification_id, notification_type, " \
            " notification_regionID, notification_hostname," \
            " notification_uuid, notification_time," \
            " notification_eventID, notification_eventType," \
            " notification_detail, notification_startTime," \
            " notification_endTime, notification_tzname, " \
            " notification_daylight, notification_cluster_port," \
            " progress, recover_by, " \
            " iscsi_ip, controle_ip, " \
            " recover_to)"

        values = " VALUES " + \
                 "(\'%s\',%s,%s,%s,\'%s\',\'%s\',\'%s\',\'%s\'," \
                 "\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,%s,\"%s\"," \
                 "\'%s\',\'%s\',%s,%s,\'%s\',\'%s\',\'%s\')"

        # NOTE: The notification item 'endTime' may have a NULL value.
        #       reference : The Notification Spec for RecoveryController.
        if jsonData.get("endTime"):
            j_endTime = None
        else:
            j_endTime = jsonData.get("endTime")
        # update and deleted :not yet
        create_at = datetime.datetime.now()
        update_at = None
        delete_at = None
        deleted = 0

	# noneToNull
	def noneToNull(column):
	    if column is None:
		return "NULL"
	    else:
		return column

        # progress 0:not yet
        progress = 0
        # From /etc/hosts
        # NOTE: Hosts hostname suffix is
        # undetermined("_data_line","_control_line")
        try:
            iscsi_ip = None
            controle_ip = socket.gethostbyname(
                jsonData.get("hostname"))
        except Exception as e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0005",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            self.rc_util.syslogout(e.message, syslog.LOG_ERR)

            raise e

        # Insert to notification_list DB.
        try:
            recover_to = None
            if recover_by == 0:

                recover_to = self._get_reserve_node_from_reserve_list_db(
                    con_args,
                    jsonData.get("cluster_port"),
                    jsonData.get("hostname"))

                # If reserve node is None, set progress 3.
                if recover_to is None:
                    progress = 3

            with MySQLdb.connect(**con_args) as cursor:
                sql_operation = "INSERT INTO notification_list " + columns
                sql_values = values % (
                    create_at, noneToNull(update_at), noneToNull(delete_at), deleted,
                    jsonData.get("id"), jsonData.get("type"),
                    jsonData.get("regionID"), jsonData.get("hostname"),
                    jsonData.get("uuid"), jsonData.get("time"),
                    jsonData.get("eventID"), jsonData.get("eventType"),
                    jsonData.get("detail"), noneToNull(jsonData.get("startTime")),
                    noneToNull(j_endTime), jsonData.get("tzname"),
                    jsonData.get("daylight"), jsonData.get("cluster_port"),
                    progress, recover_by, iscsi_ip, controle_ip, recover_to
                )

                sql = "%s%s" % (sql_operation, sql_values)

                self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0006",
                                          syslog.LOG_INFO)
                self.rc_util.syslogout("SQL=" + str(sql), syslog.LOG_INFO)

                cursor.execute(sql)

                sql = ("select * from reserve_list "
                       "where deleted=0 and hostname='%s'"
                      ) % (jsonData.get("hostname"))

                cnt = cursor.execute(sql)
                if cnt > 0:
                    sql = ("update reserve_list "
                           "set deleted=1, delete_at='%s' "
                           "where hostname='%s'"
                          ) % (datetime.datetime.now(),
                               jsonData.get("hostname"))
                    cursor.execute(sql)

            ret_dic = {
                "create_at": create_at,
                "update_at": update_at,
                "delete_at": delete_at,
                "deleted": deleted,
                "notification_id": jsonData.get("id"),
                "notification_type": jsonData.get("type"),
                "notification_regionID": jsonData.get("regionID"),
                "notification_hostname": jsonData.get("hostname"),
                "notification_uuid": jsonData.get("uuid"),
                "notification_time": jsonData.get("time"),
                "notification_eventID": jsonData.get("eventID"),
                "notification_eventType": jsonData.get("eventType"),
                "notification_detail": jsonData.get("detail"),
                "notification_startTime": jsonData.get("startTime"),
                "notification_endTime": j_endTime,
                "notification_tzname": jsonData.get("tzname"),
                "notification_daylight": jsonData.get("daylight"),
                "notification_cluster_port": jsonData.get("cluster_port"),
                "progress": progress,
                "recover_by": recover_by,
                "iscsi_ip": iscsi_ip,
                "controle_ip": controle_ip,
                "recover_to": recover_to
            }

            return ret_dic

        except Exception as e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0007",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            self.rc_util.syslogout(e.message, syslog.LOG_ERR)

            raise e

    def _get_reserve_node_from_reserve_list_db(self,
                                               con_args,
                                               cluster_port,
                                               notification_hostname):
        """
        Get reserve node, check it in use and change to 'enable'.
        :param: con_args: args database connection.
        :param: cluster_port: select keys, cluster port number.
        :return: hostname: Host name of the spare node machine
                            (obtained from the spare node list table)

        """

        csr = None
        condb = None
        try:
            # check it in use
            condb = MySQLdb.connect(**con_args)
            csr = condb.cursor(MySQLdb.cursors.DictCursor)
            sql = ("select id,hostname from reserve_list "
                   "where deleted=0 and cluster_port='%s' and hostname!='%s' "
                   "order by create_at asc limit 1 for update"
                  ) % (cluster_port, notification_hostname)

            cnt = csr.execute(sql)
            if cnt == 0:
                self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0008",
                                          syslog.LOG_WARNING)
                msg = "The reserve node not exist in reserve_list DB."
                self.rc_util.syslogout(msg, syslog.LOG_WARNING)
                hostname = None
            if cnt == 1:
                reserves = csr.fetchone()
                hostname = reserves.get('hostname')

        except Exception as e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0010",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            self.rc_util.syslogout(e.message, syslog.LOG_ERR)

            raise e

        finally:
            if csr:
                csr.close()

            if condb:
                condb.close()
        return hostname

    def _delete_reserve_node(self, con_args, cluster_port):
        """
        Delete reserve node.
        :param: con_args: args database connection.
        :param: cluster_port: select keys, cluster port number.
        """
        condb = None
        csr = None

        try:
            # check it in use
            condb = MySQLdb.connect(**con_args)
            csr = condb.cursor(MySQLdb.cursors.DictCursor)

            delete_at = datetime.datetime.now()
            sql = "update reserve_list set deleted=1 , " \
                  "delete_at='%s' " \
                  "where cluster_port='%s'" \
                  % (delete_at, cluster_port)

            self.rc_util.syslogout_ex(
                "RecoveryControllerUtilDb_0020", syslog.LOG_INFO)
            self.rc_util.syslogout(
                "SQL=" + str(sql), syslog.LOG_INFO)

            csr.execute(sql)
            condb.commit()

        except Exception as e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0021",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            self.rc_util.syslogout(e.message, syslog.LOG_ERR)

            raise e

        finally:
            if csr:
                csr.close()

            if condb:
                condb.close()

    def update_notification_list_db(self, key, value, notification_id):
        """
        Notification list table update
        :param :key: Update column name
        :param :value: Updated value
        :param :notification_id: Notification ID
                (updated narrowing condition of notification list table)
        """

        try:
            conf_db_dic = self.rc_config.get_value('db')
            conf_log_dic = self.rc_config.get_value('log')
            # Connect db
            db = MySQLdb.connect(host=conf_db_dic.get("host"),
                                 db=conf_db_dic.get("name"),
                                 user=conf_db_dic.get("user"),
                                 passwd=conf_db_dic.get("passwd"),
                                 charset=conf_db_dic.get("charset"))

            log_level = conf_log_dic.get("log_level")

            # Execute SQL
            cursor = db.cursor()

            sql = "UPDATE notification_list SET %s = %s " \
                  "WHERE notification_id = '%s'" \
                  % (key, value, notification_id)

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0011",
                                      syslog.LOG_INFO)
            self.rc_util.syslogout("SQL=" + sql, syslog.LOG_INFO)

            cursor.execute(sql)

            if key == "progress" and value == 2:
                sql = "UPDATE notification_list SET delete_at = '%s' " \
                      "WHERE notification_id = '%s'" \
                      % (datetime.datetime.now(), notification_id)
            else:
                sql = "UPDATE notification_list SET update_at = '%s' " \
                      "WHERE notification_id = '%s'" \
                      % (datetime.datetime.now(), notification_id)

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0012",
                                      syslog.LOG_INFO)
            self.rc_util.syslogout("SQL=" + sql, syslog.LOG_INFO)

            cursor.execute(sql)

            db.commit()

            # db connection close
            cursor.close()
            db.close()

        except KeyError:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0013",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : KeyError in update_notification_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise KeyError

        except MySQLdb.Error:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0014",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : MySQLdb.Error in update_notification_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise MySQLdb.Error

    def update_vm_list_db(self, key, value, uuid):
        """
        VM list table update
        :param :key: Update column name
        :param :value: Updated value
        :param :uuid: VM of uuid (updated narrowing condition of VM list table)
        """

        try:
            conf_db_dic = self.rc_config.get_value('db')
            conf_log_dic = self.rc_config.get_value('log')
            # Connect db
            db = MySQLdb.connect(host=conf_db_dic.get("host"),
                                 db=conf_db_dic.get("name"),
                                 user=conf_db_dic.get("user"),
                                 passwd=conf_db_dic.get("passwd"),
                                 charset=conf_db_dic.get("charset"))

            log_level = conf_log_dic.get("log_level")

            # Execute SQL
            cursor = db.cursor(MySQLdb.cursors.DictCursor)

            sql = "SELECT id FROM vm_list " \
                  "WHERE uuid = '%s' ORDER BY create_at DESC LIMIT 1" \
                  % (uuid)

            cursor.execute(sql)
            result = cursor.fetchone()
            result_id = result.get('id')

            sql = (("UPDATE vm_list SET %s = %s WHERE id = '%s'")
                   % (key,
                      value,
                      result_id))

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0015",
                                      syslog.LOG_INFO)
            self.rc_util.syslogout("SQL=" + sql, syslog.LOG_INFO)

            cursor.execute(sql)

            if key == "progress" and value == 2:
                sql = (("UPDATE vm_list SET delete_at = '%s' WHERE id = '%s'")
                       % (datetime.datetime.now(), result_id))
            else:
                sql = (("UPDATE vm_list SET update_at = '%s' WHERE id = '%s'")
                       % (datetime.datetime.now(), result_id))

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0016",
                                      syslog.LOG_INFO)
            self.rc_util.syslogout("SQL=" + sql, syslog.LOG_INFO)

            cursor.execute(sql)

            db.commit()

            # db connection close
            cursor.close()
            db.close()

        except KeyError:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0017",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : KeyError in update_notification_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise KeyError

        except MySQLdb.Error:

            self.rc_util.syslogout_ex("RecoveryControllerUtilDb_0018",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "Exception : MySQLdb.Error in update_notification_list_db()."
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise MySQLdb.Error


class RecoveryControllerUtilApi(object):

    """
    API-related utility classes related to VM recovery control
    """

    def __init__(self, config_object):
        self.rc_config = config_object
        self.rc_util = RecoveryControllerUtil(self.rc_config)

    def do_instance_show(self, uuid):
        """
        API-instance_show. Edit the body of the curl is
        performed using the nova client.
        :uuid : Instance id to be used in nova cliant curl.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "GET"
            # Set nova_variable_url
            nova_variable_url = "/servers/" + uuid
            # Set nova_body
            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url)

        except:

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0001",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_instance_stop(self, uuid):
        """
        API-stop. Edit the body of the curl is performed using the nova client.
        :param :uuid : Instance id to be used in nova cliant curl.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "POST"
            # Set nova_variable_url
            nova_variable_url = "/servers/" + uuid + "/action"
            # Set nova_body
            nova_body = "{\"os-stop\" : null}"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url,
                                                          nova_body)

        except:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0002",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_body=" + nova_body + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_instance_start(self, uuid):
        """
        API-start. Edit the body of the curl
        is performed using the nova client.
        :uuid : Instance id to be used in nova cliant curl.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "POST"
            # Set nova_variable_url
            nova_variable_url = "/servers/" + uuid + "/action"
            # Set nova_body
            nova_body = "{\"os-start\" : null}"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url,
                                                          nova_body)

        except:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0003",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_body=" + nova_body + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_instance_reset(self, uuid):
        """
        API-reset. Edit the body of the curl
        is performed using the nova client.
        :uuid : Instance id to be used in nova cliant curl.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "POST"
            # Set nova_variable_url
            nova_variable_url = "/servers/" + uuid + "/action"
            # Set nova_body
            nova_body = "{\"os-resetState\":{\"state\":\"error\"}}"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url,
                                                          nova_body)

        except:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0004",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_body=" + nova_body + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_hypervisor_servers(self, hypervisor_hostname):
        """
        API_hypervisor_servers. Edit the body of the curl is
        performed using the nova client.
        :hypervisor_hostname : The name of the host that runs the hypervisor.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "GET"
            # Set nova_variable_url
            nova_variable_url = "/os-hypervisors/" + \
                hypervisor_hostname + "/servers"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url)

        except:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0005",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_host_maintenance_mode(self, hostname, mode):
        """
        API_host_maintenance_mode.
        Edit the body of the curl is performed using the nova client.
        :hostname: Target host name
        :mode: change to 'enable'/'disable'
        :return :response_code :response code
        :return :rbody :response body(json)
        """

        nova_variable_url = ""
        nova_body = ""

        try:

            # Set nova_curl_method
            nova_curl_method = "PUT"

            # Set nova_variable_url
            if mode == "enable" or mode == "disable":
                nova_variable_url = "/os-services/" + mode
            else:
                e_msg = "mode is invalid.(mode=%s)" % (mode)
                raise Exception(e_msg)

            # Set nova_body
            nova_body = "{\"host\":\"" + hostname + \
                "\",\"binary\":\"nova-compute\"}"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url,
                                                          nova_body)

        except:

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0006",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_body=" + nova_body + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def do_instance_evacuate(self, uuid, targethost):
        """
        API-evacuate. Edit the body of the curl is performed
        using the nova client.
        :uuid : Instance id to be used in nova cliant curl.
        :targethost: The name or ID of the host where the server is evacuated.
        :return :response_code :response code
        :return :rbody :response body(json)
        """
        try:

            # Set nova_curl_method
            nova_curl_method = "POST"
            # Set nova_variable_url
            nova_variable_url = "/servers/" + uuid + "/action"
            # Set nova_body
            nova_body = "{\"evacuate\":{\"host\":\"" + \
                targethost + "\",\"onSharedStorage\":\"True\"}}"

            response_code, rbody = self._nova_curl_client(nova_curl_method,
                                                          nova_variable_url,
                                                          nova_body)

        except:

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0007",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = "[ nova_curl_method=" + nova_curl_method + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_variable_url=" + nova_variable_url + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)
            msg = "[ nova_body=" + nova_body + " ]"
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            raise

        return response_code, rbody

    def _get_x_subject_token(self, curl_response):

        x_subject_token = None
        curl_response_str = str(curl_response)
        tokenIndex_s = curl_response_str.find('X-Subject-Token') + 17
        tokenIndex_e = curl_response_str.find('\'', tokenIndex_s)
        x_subject_token = curl_response_str[tokenIndex_s:tokenIndex_e]

        return x_subject_token

    def _get_body(self, curl_response):
        return curl_response[-1]

    def _exe_curl(self, curl):

        p = subprocess.Popen(curl,
                             shell=True,
                             cwd='./',
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)

        out, err = p.communicate()

        self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0008",
                                  syslog.LOG_INFO)
        self.rc_util.syslogout("curl request:" + curl, syslog.LOG_INFO)
        self.rc_util.syslogout("curl response:" + out, syslog.LOG_INFO)

        # Only last respons.
        s_body_pos = out.rfind('\n\n')
        s_head_pos = out[:s_body_pos].rfind('HTTP')
        out = out[s_head_pos:]

        return out.splitlines()

    def _nova_curl_client(self,
                          nova_curl_method=None,
                          nova_variable_url=None,
                          nova_body=None,
                          auth_url=None,
                          admin_user=None,
                          admin_password=None,
                          domain=None,
                          project_id=None,
                          project_name=None):

        nova_client_url = None
        token = None
        response_code = None
        rbody = None

        # Check Required.
        try:
            if nova_curl_method is None:
                raise Exception("Need a nova_curl_method.")
            if nova_curl_method == "POST" \
               or nova_curl_method == "PUT" \
               or nova_curl_method == "PATCH":
                if nova_body is None:
                    e_msg = "method is %s. Need a nova_body." % (
                        nova_curl_method)
                    raise Exception(e_msg)
        except Exception, e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0009",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = e
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

            return None, None

        # Set default value for optional args.
        optinal_arg = self.rc_config.get_value('nova')
        if auth_url is None:
            auth_url = optinal_arg.get("auth_url")
        if admin_user is None:
            admin_user = optinal_arg.get("admin_user")
        if admin_password is None:
            admin_password = optinal_arg.get("admin_password")
        if domain is None:
            domain = optinal_arg.get("domain")
        if project_name is None:
            project_name = optinal_arg.get("project_name")

        # I get a token of admin.
        nova_client_url, token, project_id, response_code \
            = self._get_token_admin(auth_url,
                                    domain,
                                    admin_user,
                                    admin_password,
                                    project_name)

        # Get the admintoken by the project (tenant_id) in scope in the case of
        # non-GET
        if nova_curl_method != "GET":

            nova_client_url, response_code, rbody \
                = self._get_detail(nova_client_url,
                                   nova_variable_url,
                                   token)

            nova_client_url, token, project_id, response_code \
                = self._get_token_project_scope(auth_url,
                                                domain,
                                                admin_user,
                                                admin_password,
                                                project_id)

        # Run the Objective curl
        response_code, rbody \
            = self._run_curl_objective(nova_curl_method,
                                       nova_client_url,
                                       nova_variable_url,
                                       nova_body,
                                       token)

        return response_code, rbody

    def _get_token_admin(self,
                         auth_url,
                         domain,
                         admin_user,
                         admin_password,
                         project_name):

        response_code = None

        # Make curl for get token.
        token_url = "%s/v3/auth/tokens" % (auth_url)
        token_body = "{ \"auth\": { \"identity\": { \"methods\": " \
                     "[ \"password\" ], \"password\": { \"user\":" \
                     "{ \"domain\": { \"name\": \"%s\" }, \"name\": " \
                     "\"%s\", \"password\": \"%s\" } } }, \"scope\": " \
                     "{ \"project\": { \"domain\": { \"name\": \"%s\" }, " \
                     "\"name\": \"%s\"} } } }" \
                     % (domain, admin_user, admin_password, domain,
                        project_name)

        conf_dic = self.rc_config.get_value('recover_starter')
        api_max_retry_cnt = conf_dic.get('api_max_retry_cnt')
        api_retry_interval = conf_dic.get('api_retry_interval')

        token_curl = "curl --retry %s --retry-delay %s " \
                     "-i '%s' -X POST -H \"Accept: application/json\" " \
                     "-H \"Content-Type: application/json\" -d '%s'" \
                     % (api_max_retry_cnt, api_retry_interval, token_url,
                        token_body)

        # Get token id.
        token_get_res = self._exe_curl(token_curl)

        if len(token_get_res) == 0:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0016",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("exec curl command failure", syslog.LOG_ERR)
            raise Exception("exec curl command failure")

        # Token acquisition
        token = self._get_x_subject_token(token_get_res)

        for line in token_get_res:
            if line.find("HTTP") == 0:
                response_code = line.split(" ")[1]

        if response_code != "201":

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0010",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("token acquisition failure", syslog.LOG_ERR)
            raise Exception("token acquisition failure")

        # Response body acquisition
        token_res_body = self._get_body(token_get_res)
        res_json = json.loads(token_get_res[-1])
        project_id = res_json.get("token").get("project").get("id")

        catalog_list = res_json.get("token").get("catalog")

        for catalog in catalog_list:
            name = catalog.get("name")
            if name == "nova":
                endpoints = catalog.get("endpoints")
                for endpoint in endpoints:
                    interface = endpoint.get("interface")
                    if interface == "admin":
                        nova_client_url = endpoint.get("url")

        return nova_client_url, token, project_id, response_code

    def _get_detail(self,
                    nova_client_url,
                    nova_variable_url,
                    token):

        rbody = None
        response_code = None

        # Join variable url.
        if nova_variable_url is not None:
            nova_client_url = "%s%s" % (nova_client_url, "/servers/detail")

        conf_dic = self.rc_config.get_value('recover_starter')
        api_max_retry_cnt = conf_dic.get('api_max_retry_cnt')
        api_retry_interval = conf_dic.get('api_retry_interval')

        nova_client_curl = "curl --retry %s --retry-delay %s " \
                           "-i \"%s\" -X GET " \
                           "-H \"Accept: application/json\" " \
                           "-H \"Content-Type: application/json\" " \
                           "-H \"X-Auth-Token: %s\"" \
                           % (api_max_retry_cnt, api_retry_interval,
                              nova_client_url, token)
        nova_exe_res = self._exe_curl(nova_client_curl)

        if len(nova_exe_res) == 0:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0017",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("exec curl command failure", syslog.LOG_ERR)
            raise Exception("exec curl command failure")

        for line in nova_exe_res:
            if line.find("HTTP") == 0:
                response_code = line.split(" ")[1]

        if response_code != "200":
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0011",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("detail acquisition failure",
                                   syslog.LOG_ERR)
            raise Exception("detail acquisition failure")
        else:
            try:
                for line in nova_exe_res:
                    if "Content-Length" in line:
                        length = line.split(" ")[-1]

                        if length != "0":
                            rbody = self._get_body(nova_exe_res)

                        else:
                            raise Exception("Is not enough response body.")

            except Exception, e:

                self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0012",
                                          syslog.LOG_ERR)
                error_type, error_value, traceback_ = sys.exc_info()
                tb_list = traceback.format_tb(traceback_)
                self.rc_util.syslogout(error_type, syslog.LOG_ERR)
                self.rc_util.syslogout(error_value, syslog.LOG_ERR)
                for tb in tb_list:
                    self.rc_util.syslogout(tb, syslog.LOG_ERR)

                msg = e
                self.rc_util.syslogout(msg, syslog.LOG_ERR)

        return nova_client_url, response_code, rbody

    def _get_token_project_scope(self,
                                 auth_url,
                                 domain,
                                 admin_user,
                                 admin_password,
                                 project_id):

        response_code = None

        # Make curl for get token.
        token_url = "%s/v3/auth/tokens" % (auth_url)
        token_body = "{ \"auth\": { \"identity\": { \"methods\": " \
                     "[ \"password\" ], \"password\": { \"user\": " \
                     "{ \"domain\": { \"name\": \"%s\" }, \"name\": \"%s\", " \
                     "\"password\": \"%s\" } } }, \"scope\": { \"project\": " \
                     "{ \"id\": \"%s\"} } } }" \
                     % (domain, admin_user, admin_password, project_id)

        conf_dic = self.rc_config.get_value('recover_starter')
        api_max_retry_cnt = conf_dic.get('api_max_retry_cnt')
        api_retry_interval = conf_dic.get('api_retry_interval')

        token_curl = "curl --retry %s --retry-delay %s " \
                     "-i '%s' -X POST -H \"Accept: application/json\" " \
                     "-H \"Content-Type: application/json\" -d '%s'" \
                     % (api_max_retry_cnt, api_retry_interval,
                        token_url, token_body)

        # Get token id.
        token_get_res = self._exe_curl(token_curl)

        if len(token_get_res) == 0:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0018",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("exec curl command failure", syslog.LOG_ERR)
            raise Exception("exec curl command failure")

        for line in token_get_res:
            if line.find("HTTP") == 0:
                response_code = line.split(" ")[1]

        if response_code != "201":
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0013",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("token acquisition failure", syslog.LOG_ERR)
            raise Exception("token acquisition failure")

        # Token acquisition
        token = self._get_x_subject_token(token_get_res)

        # Response body acquisition
        token_res_body = self._get_body(token_get_res)

        res_json = json.loads(token_get_res[-1])

        project_id = res_json.get("token").get("project").get("id")
        catalog_list = res_json.get("token").get("catalog")

        for catalog in catalog_list:
            name = catalog.get("name")
            if name == "nova":
                endpoints = catalog.get("endpoints")
                for endpoint in endpoints:
                    interface = endpoint.get("interface")
                    if interface == "admin":
                        nova_client_url = endpoint.get("url")

        return nova_client_url, token, project_id, response_code

    def _run_curl_objective(self,
                            nova_curl_method,
                            nova_client_url,
                            nova_variable_url,
                            nova_body,
                            token):

        rbody = None
        response_code = None

        # Join variable url.
        if nova_variable_url is not None:
            nova_client_url = "%s%s" % (nova_client_url, nova_variable_url)

        conf_dic = self.rc_config.get_value('recover_starter')
        api_max_retry_cnt = conf_dic.get('api_max_retry_cnt')
        api_retry_interval = conf_dic.get('api_retry_interval')

        nova_client_curl = "curl --retry %s --retry-delay %s " \
                           "-i \"%s\" -X %s -H \"Content-Type: " \
                           "application/json\" -H \"X-Auth-Token: %s\"" \
                           % (api_max_retry_cnt, api_retry_interval,
                              nova_client_url, nova_curl_method, token)

        if nova_body is not None:
            nova_client_curl = "%s -d '%s'" % (nova_client_curl, nova_body)

        nova_exe_res = self._exe_curl(nova_client_curl)

        if len(nova_exe_res) == 0:
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0019",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("exec curl command failure", syslog.LOG_ERR)
            raise Exception("exec curl command failure")

        for line in nova_exe_res:
            if line.find("HTTP") == 0:
                response_code = line.split(" ")[1]

        if response_code != "200" and response_code != "202":
            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0014",
                                      syslog.LOG_ERR)
            self.rc_util.syslogout("exec curl command failure", syslog.LOG_ERR)

        try:
            for line in nova_exe_res:
                if "Content-Length" in line:
                    length = line.split(" ")[-1]
            rbody = self._get_body(nova_exe_res)

            if len(rbody) != int(length):
                raise Exception("Bad response body.")

        except Exception, e:

            self.rc_util.syslogout_ex("RecoveryControllerUtilApi_0015",
                                      syslog.LOG_ERR)
            error_type, error_value, traceback_ = sys.exc_info()
            tb_list = traceback.format_tb(traceback_)
            self.rc_util.syslogout(error_type, syslog.LOG_ERR)
            self.rc_util.syslogout(error_value, syslog.LOG_ERR)
            for tb in tb_list:
                self.rc_util.syslogout(tb, syslog.LOG_ERR)

            msg = e
            self.rc_util.syslogout(msg, syslog.LOG_ERR)

        return response_code, rbody


class RecoveryControllerUtil(object):

    """
    Other utility classes for VM recovery control
    """

    def __init__(self, config_object):
        self.rc_config = config_object

    def syslogout_ex(self, msgid, logOutLevel):
        """
        I output the log to a given log file
        :msgid : Log output message ID(Monitoring message)
        :logOutLevel: Log output level
        """

        monitoring_message = "--MonitoringMessage--ID:[%s]" % (msgid)
        self.syslogout(monitoring_message, logOutLevel)

    def syslogout(self, msg, logOutLevel):
        """
        I output the log to a given log file
        :msg : Log output messages
        :logOutLevel: Log output level
        """

        config_log_dic = self.rc_config.get_value('log')
        logLevel = config_log_dic.get("log_level")

        # Output log
        arg0 = os.path.basename(sys.argv[0])
        host = socket.gethostname()

        logger = logging.getLogger()

        wk_setLevel = ""
        if logLevel == syslog.LOG_DEBUG:
            wk_setLevel = logging.DEBUG
        elif logLevel == syslog.LOG_INFO or logLevel == syslog.LOG_NOTICE:
            wk_setLevel = logging.INFO
        elif logLevel == syslog.LOG_WARNING:
            wk_setLevel = logging.WARNING
        elif logLevel == syslog.LOG_ERR:
            wk_setLevel = logging.ERROR
        elif logLevel == syslog.LOG_CRIT or logLevel == syslog.LOG_ALERT or \
                logLevel == syslog.LOG_EMERG:
            wk_setLevel = logging.CRITICAL
        else:
            wk_setLevel = logging.ERROR

        logger.setLevel(wk_setLevel)
        f = "%(asctime)s " + host + \
            " masakari(%(process)d): %(levelname)s: %(message)s'"
        formatter = logging.Formatter(fmt=f, datefmt='%b %d %H:%M:%S')
        fh = logging.FileHandler(
            filename='/var/log/masakari/masakari-controller.log')
        fh.setLevel(wk_setLevel)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

        if logOutLevel == syslog.LOG_DEBUG:
            logger.debug(msg)
        elif logOutLevel == syslog.LOG_INFO or \
                logOutLevel == syslog.LOG_NOTICE:
            logger.info(msg)
        elif logOutLevel == syslog.LOG_WARNING:
            logger.warn(msg)
        elif logOutLevel == syslog.LOG_ERR:
            logger.error(msg)
        elif logOutLevel == syslog.LOG_CRIT or \
                logOutLevel == syslog.LOG_ALERT or \
                logOutLevel == syslog.LOG_EMERG:
            logger.critical(msg)
        else:
            logger.debug(msg)

        logger.removeHandler(fh)
