package com.alibaba.druid.bvt.sql.odps;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.parser.Lexer;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLType;
import com.alibaba.druid.support.opds.udf.SqlTypeUDF;
import junit.framework.TestCase;

public class ScanSQLTypeV2Test extends TestCase {
    protected SqlTypeUDF udf = new SqlTypeUDF();

    public void test_sqlTypeV2() throws Exception {
        String sql = "create TABLE IF NOT EXISTS tmp_ventes_20210218_1430 LIFECYCLE 1 AS \n" +
                "select  did, ";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.CREATE_TABLE_AS_SELECT, sqlType);
    }

    public void test_sqlTypeV2_1() throws Exception {
        String sql = "CREATE FUNCTION COMBINE_PB_ONLINE as 'UDTFCombinePbV2.CombinePb' USING 'UDTFCombinePbV2.py, fg.json'";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.CREATE_FUNCTION, sqlType);
    }

    public void test_sqlTypeV2_2() throws Exception {
        String sql = "WITH\n" +
                "    hub  AS\n" +
                "(\n" +
                "    SELECT  a.id, a.name  FROM  eserve_js.tollinterval a\n" +
                "    LEFT JOIN eserve_js_dev.map_ramp b ON a.id = b.id\n" +
                "    LEFT JOIN eserve_js_dev.map_road c ON a.id = c.id\n" +
                "    WHERE b.id IS NULL AND c.id IS NULL\n" +
                ")\n" +
                "SELECT * FROM hub";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.WITH, sqlType);
    }

    public void test_sqlTypeV2_3() throws Exception {
        String sql = "DROP FUNCTION COMBINE_PB_ONLINE";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.DROP_FUNCTION, sqlType);
    }

    public void test_sqlTypeV2_4() throws Exception {
        String sql = "INSERT INTO employee VALUES \n" +
                "(13,'Mari',51,'M'),\n" +
                "(14,'Pat',34,'F');";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.INSERT_INTO_VALUES, sqlType);
    }

    public void test_sqlTypeV2_5() throws Exception {
        String sql = "\n" +
                "add table xxxx partition(ds='20210306') as 'search_tablebert_output_v_vocab' -f";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_TABLE, sqlType);
    }

    public void test_sqlTypeV2_6() throws Exception {
        String sql = "\n" +
                "tunnel download xx_dev.xxx tem_app_list_dy.txt";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.TUNNEL_DOWNLOAD, sqlType);
    }

    public void test_sqlTypeV2_7() throws Exception {
        String sql = "\n" +
                "upload xx \n" +
                "FROM http://xxx/res?id=163890713";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.UPLOAD, sqlType);
    }

    public void test_sqlTypeV2_8() throws Exception {
        String sql = "whoami";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.WHOAMI, sqlType);
    }

    public void test_sqlTypeV2_9() throws Exception {
        String sql = "count prod_ads.bus_parking_lot;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.COUNT, sqlType);
    }

    public void test_sqlTypeV2_10() throws Exception {
        String sql = "add statistic dataexploration_2495i2wa0j5i27ui9i5t_2021080920 TABLE_COUNT ;;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_STATISTIC, sqlType);
    }

    public void test_sqlTypeV2_11() throws Exception {
        String sql = "--name:vt_vdm_mitem_finish\n" +
                "--author:alvin.xun\n" +
                "--create time:2019-12-13 15:40;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.EMPTY, sqlType);
    }

    public void test_sqlTypeV2_12() throws Exception {
        String sql = "】\n" +
                "select * from kof_moneyflow where rid='248900_22' and platform = 'korea' and dt between '2021-06-07' and '2021-06-07';";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SELECT, sqlType);
    }

    public void test_sqlTypeV2_13() throws Exception {
        String sql = "ALTER TABLE bg_finance.stg_tcloud_aa_account_tmp DROP IF EXISTS PARTITION (db='uftdata190419_415404');";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ALTER_TABLE, sqlType);
    }

    public void test_sqlTypeV2_14() throws Exception {
        String sql = ";";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.EMPTY, sqlType);
    }

    public void test_sqlTypeV2_15() throws Exception {
        String sql = " set odps.mr.run.mode=sql;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SET, sqlType);
    }

    public void test_sqlTypeV2_16() throws Exception {
        String sql = " set odps.mr.run.mode=sql;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SET, sqlType);
    }

    public void test_sqlTypeV2_17() throws Exception {
        String sql = "list users;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.LIST_USERS, sqlType);
    }

    public void test_sqlTypeV2_18() throws Exception {
        String sql = "SHOW STATISTIC_LIST s_aegis_netstat_log_mi;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_STATISTIC_LIST, sqlType);
    }

    public void test_sqlTypeV2_19() throws Exception {
        String sql = "SET LABEL P3 TO TABLE adm_ot_bi_ana_orchard_kuanbiao10_20200910_di (gender);";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SET_LABEL, sqlType);
    }

    public void test_sqlTypeV2_20() throws Exception {
        String sql = "LIST TENANT ROLES;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.LIST_TENANT_ROLES, sqlType);
    }

    public void test_sqlTypeV2_22() throws Exception {
        String sql = "alter view adm_ddm_app_mct_insight_result_promotion_cahnnel_config set TBLPROPERTIES('comment' = '数据更新20210809');";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ALTER_VIEW, sqlType);
    }

    public void test_sqlTypeV2_21() throws Exception {
        String sql = "create  table if not exists ods_stat_summary_db_delta_hh(cluster_name string comment '',node string comment '',ip_port string comment '',role string comment '',db_name string comment '',db_conn string comment '',db_size_gb string comment '',lastest_modified_time string comment '',gmt_create string comment '') partitioned by(dt string,hour string) LIFECYCLE 3650;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.CREATE_TABLE, sqlType);
    }

    public void test_sqlTypeV2_23() throws Exception {
        String sql = "list accountproviders;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.LIST_ACCOUNTPROVIDERS, sqlType);
    }

    public void test_sqlTypeV2_24() throws Exception {
        String sql = "drop role";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.DROP_ROLE, sqlType);
    }

    public void test_sqlTypeV2_25() throws Exception {
        String sql = "show create table ut_history;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_CREATE_TABLE, sqlType);
    }


    public void test_sqlTypeV2_26() throws Exception {
        String sql = "SHOW CHANGELOGS FOR TABLE s_ipm_trade_order_dup_check_all;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_CHANGELOGS, sqlType);
    }

    public void test_sqlTypeV2_27() throws Exception {
        String sql = "SHOW ACL FOR shuxingquanrenresult01181542 ON TYPE TABLE;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_ACL, sqlType);
    }

    public void test_sqlTypeV2_28() throws Exception {
        String sql = "show grant for RAM$cainiao_wutong:wb-lt893799;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_GRANT, sqlType);
    }

    public void test_sqlTypeV2_30() throws Exception {
        String sql = "show package pakg_1071500_l2;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_PACKAGE, sqlType);
    }

    public void test_sqlTypeV2_29() throws Exception {
        String sql = "show tables；;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_TABLES, sqlType);
    }

    public void test_sqlTypeV2_31() throws Exception {
        String sql = "show variables  like '%profi%';";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_VARIABLES, sqlType);
    }

    public void test_sqlTypeV2_32() throws Exception {
        String sql = "ADD accountprovider RAM;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_ACCOUNTPROVIDER, sqlType);
    }

    public void test_sqlTypeV2_33() throws Exception {
        String sql = "add function wo1_336005 to package alinlp_udf;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_FUNCTION, sqlType);
    }

    public void test_sqlTypeV2_34() throws Exception {
        String sql = "add function wo1_336005 to package alinlp_udf;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_FUNCTION, sqlType);
    }

    public void test_sqlTypeV2_35() throws Exception {
        String sql = "add resource wo1_336005.py to package alinlp_udf;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_RESOURCE, sqlType);
    }

    public void test_sqlTypeV2_36() throws Exception {
        String sql = "ADD TRUSTEDPROJECT mysecods_dev;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.ADD_TRUSTEDPROJECT, sqlType);
    }

    public void test_sqlTypeV2_37() throws Exception {
        String sql = "create package dsmp_odps_ant_ext2gentcdm_dev_0;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.CREATE_PACKAGE, sqlType);
    }

    public void test_sqlTypeV2_38() throws Exception {
        String sql = "list temporary output;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.LIST_TEMPORARY_OUTPUT, sqlType);
    }

    public void test_sqlTypeV2_39() throws Exception {
        String sql = "-- 新库存一级账内对账\n";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.EMPTY, sqlType);
    }

    public void test_sqlTypeV2_40() throws Exception {
        String sql = "\n" +
                "\n" +
                "：create table mesa_temp_2925_44150_1_20210809_wpyvqCt5 lifecycle 3 as select * from mesa_temp_2925_44122_1_20210809_wpyvqCt5 where store_code='NGB503' AND day_id between '20200501' and '20200520' ;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.CREATE_TABLE_AS_SELECT, sqlType);
    }

    public void test_sqlTypeV2_41() throws Exception {
        String sql = "remove function emp_271719 from package alinlp_udf;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.REMOVE, sqlType);
    }

    public void test_sqlTypeV2_42() throws Exception {
        String sql = "show role grant \n" +
                "bsj65s8s6ahf@aliyun.com;";

        Lexer lexer = SQLParserUtils.createLexer(sql, DbType.odps);
        SQLType sqlType = lexer.scanSQLTypeV2();
        assertEquals(SQLType.SHOW_ROLE, sqlType);
    }

    public void test_sqlTypeV2_udf_01() throws Exception {
        String sql = "ALTER TABLE DWS_ITEM_APPLY2DELIV_LIST_DF\n" +
                "\tCHANGE COLUMN DELIVERY_PDOTYPE RENAME TO DELIVERY_ORDER_TYPE;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE_RENAME_COLUMN.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_02() throws Exception {
        String sql = "alter table adm_ylb_fncc_prch_insr_fud_file merge if exists \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='00'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='01'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='02'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='03'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='04'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='05'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='06'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='07'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='08'), \n" +
                "partition(dt='20210725', hour='12', inst_code='htffd', part='09')\n" +
                "overwrite partition(dt='20210725',hour='24',inst_code='htffd', part='09');";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE_MERGE_PARTITION.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_03() throws Exception {
        String sql = "ALTER TABLE ytsoku.dws_soku_engine_show_tags_intervene_20190228 TOUCH;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE_TOUCH.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_04() throws Exception {
        String sql = "alter table dws_soku_engine_smart_lab_ocr_text_clear_all partition(ds='20210809') disable lifecycle;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE_DISABLE_LIFECYCLE.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_05() throws Exception {
        String sql = "alter table tmp_smart_dw_metric_v2_dim_employee_fulltime_1007733942 changeowner to 'ALIYUN$DXP_95797887@aliyun.com';";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE_CHANGE_OWNER.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_06() throws Exception {
        String sql = "set odps.sql.allow.fullscan=true; SELECT ARE_T_1_.`id` AS T_A98_2_, ARE_T_1_.`class` AS T_AB6_3_, ARE_T_1_.`area_name` AS T_A3E_4_, ARE_T_1_.`biz_name` AS T_A13_5_, ARE_T_1_.`order_level` AS T_A26_6_, ARE_T_1_.`utm_class1` AS T_A6A_7_, ARE_T_1_.`utm_class2` AS T_ABB_8_, TO_CHAR(ARE_T_1_.`audit_time`, 'yyyyMMdd') AS T_AC9_9_, ARE_T_1_.`order_id` AS T_AAF_10_, SUM(ARE_T_1_.`total_price`) AS T_A93_11_, SUM(ARE_T_1_.`receivable`) AS T_AA2_12_ FROM `daoxila_dw`.`ads_bi_confirm_order` ARE_T_1_ WHERE TO_CHAR(ARE_T_1_.`audit_time`, 'yyyyMMdd') >= '20210801' AND TO_CHAR(ARE_T_1_.`audit_time`, 'yyyyMMdd') <= '20210831' AND ARE_T_1_.`area_name` IN ('上海市', '北京市', '杭州市') GROUP BY ARE_T_1_.`id`, ARE_T_1_.`class`, ARE_T_1_.`area_name`, ARE_T_1_.`biz_name`, ARE_T_1_.`order_level`, ARE_T_1_.`utm_class1`, ARE_T_1_.`utm_class2`, TO_CHAR(ARE_T_1_.`audit_time`, 'yyyyMMdd'), ARE_T_1_.`order_id`  LIMIT 10001;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SELECT.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_07() throws Exception {
        String sql = "alter table global_algo.nebula_nebula_nebula_job_4520566_sample_generate_tmp_1 clustered by (c0) into 50 shards;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.ALTER_TABLE.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_08() throws Exception {
        String sql = "set set odps.sql.udf.jvm.memory=4096;\nselect 1;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SELECT.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_09() throws Exception {
        String sql = "DROP MATERIALIZED VIEW IF EXISTS adm_csn_exp_tmhw_lgt_ord_cpf_fulfill_detail_materview;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.DROP_MATERIALIZED_VIEW.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_10() throws Exception {
        String sql = "set project odps.sql.allow.fullscan=true;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SET_PROJECT.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_11() throws Exception {
        String sql = "set.odps.mcqa.disable=true;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SET.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_12() throws Exception {
        String sql = "SHOW TABLES IN  dy_ads;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SHOW_TABLES.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_13() throws Exception {
        String sql = "set odps.sql.hive.compatible=true;\n" +
                "        load into table raw_promotion_oceanengine_hour partition(ds='20210810')\n" +
                "        from location 'oss://LTAI4Fkb3Y8GWgQcs4Kcuspu:rYGHPDTVXZl0AE359Eqo1kKguSbO4u@oss-cn-hangzhou-internal.aliyuncs.com/hippo-warehouse/tmp/raw_promotion_oceanengine_hour/delta-fetch-oceanengine-creative-promotion-consumer-task-12rhw6/20210810/'\n" +
                "        ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'\n" +
                "        STORED AS TEXTFILE;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.LOAD.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_14() throws Exception {
        String sql = "purge all 24;PURGE table ads_gd_session_visualization;show recyclebin;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals("MULTI:PURGE,SHOW_RECYCLEBIN", sqlType);
    }

    public void test_sqlTypeV2_udf_15() throws Exception {
        String sql = "SHOW STATISTIC s_wm_tbk_pid PARTITION(ds = '20210809');";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SHOW_STATISTIC.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_16() throws Exception {
        String sql = "show partitions sdkv2_require_result_gc7;\n" +
                "\n" +
                "show partitions sdkv2_require_result_analyse;\n" +
                "show partitions  sdkv2_require_result_gc6;\n" +
                "\n" +
                "show partitions  sdkv2_require_result_gc5;\n" +
                "show partitions  sdkv2_require_result_gc4;\n";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SHOW_PARTITIONS.name(), sqlType);
    }

    public void test_sqlTypeV2_udf_1001() throws Exception {
        String sql = "--昨天的最后一条数据   要带上一天的uv\n" +
                "IF 15 == \"23\" \n" +
                "\n" +
                "    --将bid和nobid  union到一起\n" +
                "    WITH AllBid as (\n" +
                "        select imei,imei_md5,imei_sha1,idfa,idfa_md5,idfa_sha1,oaid,oaid_md5,oaid_sha1,android_id,android_id_md5,android_id_sha1,mac,mac_md5,mac_sha1,cookie,hour,adx_id,media_id,devtype as device_type,case os when \"android\" then 1 when \"ios\" then 2 else 0 end as os_id,req_id,imp_id,bidfloor from ods_dsp_bid_log where dt=\"20210810\"\n" +
                "        UNION ALL\n" +
                "        select imei,imei_md5,imei_sha1,idfa,idfa_md5,idfa_sha1,oaid,oaid_md5,oaid_sha1,android_id,android_id_md5,android_id_sha1,mac,mac_md5,mac_sha1,cookie,hour,adx_id,media_id,devtype as device_type,case os when \"android\" then 1 when \"ios\" then 2 else 0 end as os_id,req_id,imp_id,bidfloor from ods_dsp_nobid_log where dt=\"20210810\"\n" +
                "    )\n" +
                "\n" +
                "    INSERT INTO table rpt_dsp_media_traffic partition (dt='20210810',hour = \"23\")\n" +
                "    select rdmt.adx_id,rdmt.media_id,rdmt.device_type,rdmt.os_id,rdmt.request,rdmt.sum_base_cpm,uv.uvcount,rdmt.time from (\n" +
                "\n" +
                "        --统计当前时段的总和\n" +
                "        select adx_id,media_id,device_type,os_id,count(req_id) as request,sum(bidfloor) as sum_base_cpm,TO_DATE(20210810151000,\"yyyymmddhhmiss\") as time from (\n" +
                "            --去重 虽然多套一层  但是效率比 union高\n" +
                "            select adx_id,media_id,device_type,os_id,concat(req_id,imp_id) as req_id,bidfloor from(\n" +
                "                AllBid          \n" +
                "            )where hour = \"15\" group by adx_id,media_id,device_type,os_id,concat(req_id,imp_id),bidfloor\n" +
                "        )group by adx_id,media_id,device_type,os_id\n" +
                "\n" +
                "    ) rdmt,(\n" +
                "        select count(*) as uvcount from (\n" +
                "            select userid from (\n" +
                "                select\n" +
                "                case\n" +
                "                when imei != \"\" then imei\n" +
                "                when imei_md5 != \"\" then imei_md5\n" +
                "                when imei_sha1 != \"\" then imei_sha1\n" +
                "                when idfa != \"\" then idfa\n" +
                "                when idfa_md5 != \"\" then idfa_md5\n" +
                "                when idfa_sha1 != \"\" then idfa_sha1\n" +
                "                when oaid != \"\" then oaid\n" +
                "                when oaid_md5 != \"\" then oaid_md5\n" +
                "                when oaid_sha1 != \"\" then oaid_sha1\n" +
                "                when android_id != \"\" then android_id\n" +
                "                when android_id_md5 != \"\" then android_id_md5\n" +
                "                when android_id_sha1 != \"\" then android_id_sha1\n" +
                "                when mac != \"\" then mac\n" +
                "                when mac_md5 != \"\" then mac_md5\n" +
                "                when mac_sha1 != \"\" then mac_sha1\n" +
                "                when cookie != \"\" then cookie\n" +
                "                else \"\"\n" +
                "                end as userid\n" +
                "                from (\n" +
                "                  AllBid\n" +
                "                )\n" +
                "            )where userid != \"\" group by userid\n" +
                "        )\n" +
                "    ) uv;\n" +
                "ELSE \n" +
                "    INSERT INTO TABLE rpt_dsp_media_traffic PARTITION (dt='20210810', hour='15')\n" +
                "    --统计当前时段的总和\n" +
                "    select adx_id,media_id,device_type,os_id,count(req_id) as request,sum(bidfloor) as sum_base_cpm,0 as uv_count,TO_DATE(20210810151000,\"yyyymmddhhmiss\") as time from (\n" +
                "        --去重 虽然多套一层  但是效率比 union高\n" +
                "        select adx_id,media_id,device_type,os_id,concat(req_id,imp_id) as req_id,bidfloor from(\n" +
                "            --将bid和nobid  union到一起\n" +
                "            select adx_id,media_id,devtype as device_type,case os when \"android\" then 1 when \"ios\" then 2 else 0 end as os_id,req_id,imp_id,bidfloor from ods_dsp_bid_log where dt=\"20210810\" and hour= \"15\"\n" +
                "            UNION ALL\n" +
                "            select adx_id,media_id,devtype as device_type,case os when \"android\" then 1 when \"ios\" then 2 else 0 end as os_id,req_id,imp_id,bidfloor from ods_dsp_nobid_log where dt=\"20210810\" and hour= \"15\"\n" +
                "        ) group by adx_id,media_id,device_type,os_id,concat(req_id,imp_id),bidfloor\n" +
                "    )group by adx_id,media_id,device_type,os_id;";

        String sqlType = udf.evaluate(sql, "odps");
        assertEquals(SQLType.SCRIPT.name(), sqlType);
    }

}
