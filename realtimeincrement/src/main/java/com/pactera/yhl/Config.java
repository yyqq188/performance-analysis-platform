package com.pactera.yhl;

public class Config {


    public static final String t01bankinfoyb_ins = "KLHBASE:T01BANKINFOYB_INS";
    public static final String t01bankinfoyb_del = "KLHBASE:T01BANKINFOYB_DEL";
    public static final String[] t01bankinfoyb_rowkeys = {"code"};
    public static final String[] t01bankinfoyb_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};



    public static final String t01branchinfo_ins = "KLHBASE:T01BRANCHINFO_INS";
    public static final String t01branchinfo_del = "KLHBASE:T01BRANCHINFO_DEL";
    public static final String[] t01branchinfo_rowkeys = {"code"};
    public static final String[] t01branchinfo_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String t01teaminfo_ins = "KLHBASE:T01TEAMINFO_INS";
    public static final String t01teaminfo_del = "KLHBASE:T01TEAMINFO_DEL";
    public static final String[] t01teaminfo_rowkeys = {"code"};
    public static final String[] t01teaminfo_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String t02salesinfo_backup_ins = "KLHBASE:T02SALESINFO_BACKUP_INS";
    public static final String t02salesinfo_backup_del = "KLHBASE:T02SALESINFO_BACKUP_DEL";
    public static final String[] t02salesinfo_backup_rowkeys = {"code"};
    public static final String[] t02salesinfo_backup_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String t02salesinfo_k_ins = "KLHBASE:T02SALESINFO_K_INS";
    public static final String t02salesinfo_k_del = "KLHBASE:T02SALESINFO_K_DEL";
    public static final String[] t02salesinfo_k_rowkeys = {"code"};
    public static final String[] t02salesinfo_k_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String t02trainralation_ins = "KLHBASE:T02TRAINRALATION_INS";
    public static final String t02trainralation_del = "KLHBASE:T02TRAINRALATION_DEL";
    public static final String[] t02trainralation_rowkeys = {"code"};
    public static final String[] t02trainralation_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String t04sumpermatchassess_ins = "KLHBASE:T04SUMPERMATCHASSESS_INS";
    public static final String t04sumpermatchassess_del = "KLHBASE:T04SUMPERMATCHASSESS_DEL";
    public static final String[] t04sumpermatchassess_rowkeys = {"code"};
    public static final String[] t04sumpermatchassess_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String anychatcont_ins = "KLHBASE:ANYCHATCONT_INS";
    public static final String anychatcont_del = "KLHBASE:ANYCHATCONT_DEL";
    public static final String[] anychatcont_rowkeys = {"businessno"};
    public static final String[] anychatcont_columnNames = {"businessno","drsflag","checkdate","ischeck",
            "checkresult","fcd","flag1","flag2","flag3","flag4","operator","makedate","maketime","modifydate",
            "modifytime","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String imcont_ins = "KLHBASE:IMCONT_INS";
    public static final String imcont_del = "KLHBASE:IMCONT_DEL";
    public static final String[] imcont_rowkeys = {"data_source"};
    public static final String[] imcont_columnNames = {};







    public static final String labranchgroup_ins = "KLHBASE:LABRANCHGROUP_INS";
    public static final String labranchgroup_del = "KLHBASE:LABRANCHGROUP_DEL";
    public static final String[] labranchgroup_rowkeys = {"code"};
    public static final String[] labranchgroup_columnNames = {"agentgroup","name","managecom","upbranch","branchattr",
            "branchseries","branchtype","branchlevel","branchmanager","branchaddresscode","branchaddress","branchphone",
            "branchfax","branchzipcode","founddate","enddate","endflag","fieldflag","state","branchmanagername",
            "upbranchattr","branchjobtype","operator","makedate","maketime","modifydate","modifytime","branchtype2",
            "astartdate","branchlevelkind","trusteeship","insideflag","branchstyle","comstyle","costcenter","etl_dt",
            "etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lacom_ins = "KLHBASE:LACOM_INS";
    public static final String lacom_del = "KLHBASE:LACOM_DEL";
    public static final String[] lacom_rowkeys = {"code"};
    public static final String[] lacom_columnNames = {};

    public static final String lbappnt_ins = "KLHBASE:LBAPPNT_INS";
    public static final String lbappnt_del = "KLHBASE:LBAPPNT_DEL";
    public static final String[] lbappnt_rowkeys = {"code"};
    public static final String[] lbappnt_columnNames = {};

    public static final String lbcont_ins = "KLHBASE:LBCONT_INS";
    public static final String lbcont_del = "KLHBASE:LBCONT_DEL";
    public static final String[] lbcont_rowkeys = {"code"};
    public static final String[] lbcont_columnNames = {};

    public static final String lbinsured_ins = "KLHBASE:LBINSURED_INS";
    public static final String lbinsured_del = "KLHBASE:LBINSURED_DEL";
    public static final String[] lbinsured_rowkeys = {"code"};
    public static final String[] lbinsured_columnNames = {};

    public static final String lbpol_ins = "KLHBASE:LBPOL_INS";
    public static final String lbpol_del = "KLHBASE:LBPOL_DEL";
    public static final String[] lbpol_rowkeys = {"code"};
    public static final String[] lbpol_columnNames = {"polno","edorno","grpcontno","grppolno","contno","proposalno",
            "prtno","conttype","poltypeflag","mainpolno","masterpolno","kindcode","riskcode","riskversion","managecom",
            "agentcom","agenttype","agentcode","agentgroup","agentcode1","salechnl","handler","insuredno","insuredname",
            "insuredsex","insuredbirthday","insuredappage","insuredpeoples","occupationtype","appntno","appntname",
            "cvalidate","signcom","signdate","signtime","firstpaydate","payenddate","paytodate","getstartdate","enddate",
            "accienddate","getyearflag","getyear","payendyearflag","payendyear","insuyearflag","insuyear","acciyearflag",
            "acciyear","getstarttype","specifyvalidate","paymode","paylocation","payintv","payyears","years",
            "managefeerate","floatrate","premtoamnt","mult","standprem","prem","sumprem","amnt","riskamnt","leavingmoney",
            "endorsetimes","claimtimes","livetimes","renewcount","lastgetdate","lastloandate","lastregetdate",
            "lastedordate","lastrevdate","rnewflag","stopflag","expiryflag","autopayflag","interestdifflag","subflag",
            "bnfflag","healthcheckflag","impartflag","reinsureflag","agentpayflag","agentgetflag","livegetmode",
            "deadgetmode","bonusgetmode","bonusman","deadflag","smokeflag","remark","approveflag","approvecode",
            "approvedate","approvetime","uwflag","uwcode","uwdate","uwtime","polapplydate","appflag","polstate",
            "standbyflag1","standbyflag2","standbyflag3","operator","makedate","maketime","modifydate","modifytime",
            "waitperiod","payrulecode","ascriptionrulecode","salechnldetail","riskseqno","copys","comfeerate",
            "branchfeerate","proposalcontno","contplancode","cessamnt","stateflag","supplementaryprem","acctype",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lcaddress_ins = "KLHBASE:LCADDRESS_INS";
    public static final String lcaddress_del = "KLHBASE:LCADDRESS_DEL";
    public static final String[] lcaddress_rowkeys = {"code"};
    public static final String[] lcaddress_columnNames = {"customerno","addressno","postaladdress","zipcode","phone",
            "fax","homeaddress","homezipcode","homephone","homefax","companyaddress","companyzipcode","companyphone",
            "companyfax","mobile","mobilechs","email","bp","mobile2","mobilechs2","email2","bp2","operator","makedate",
            "maketime","modifydate","modifytime","grpname","province","city","county","etl_dt","etl_tm","etl_fg","op_ts",
            "current_ts","load_date"};

    public static final String lcappnt_ins = "KLHBASE:LCAPPNT_INS";
    public static final String lcappnt_del = "KLHBASE:LCAPPNT_DEL";
    public static final String[] lcappnt_rowkeys = {"code"};
    public static final String[] lcappnt_columnNames = {"grpcontno","contno","prtno","appntno","appntgrade","appntname",
            "appntsex","appntbirthday","appnttype","addressno","idtype","idno","nativeplace","nationality","rgtaddress",
            "marriage","marriagedate","health","stature","avoirdupois","degree","creditgrade","bankcode","bankaccno",
            "accname","joincompanydate","startworkdate","position","salary","occupationtype","occupationcode","worktype",
            "pluralitytype","smokeflag","operator","managecom","makedate","maketime","modifydate","modifytime","bmi",
            "othidtype","othidno","englishname","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lccont_ins = "KLHBASE:LCCONT_INS";
    public static final String lccont_del = "KLHBASE:LCCONT_DEL";
    public static final String[] lccont_rowkeys = {"code"};
    public static final String[] lccont_columnNames = {"grpcontno","contno","proposalcontno","prtno","conttype",
            "familytype","familyid","poltype","cardflag","managecom","executecom","agentcom","agentcode","agentgroup",
            "agentcode1","agenttype","salechnl","handler","password","appntno","appntname","appntsex","appntbirthday",
            "appntidtype","appntidno","insuredno","insuredname","insuredsex","insuredbirthday","insuredidtype",
            "insuredidno","payintv","paymode","paylocation","disputedflag","outpayflag","getpolmode","signcom",
            "signdate","signtime","consignno","bankcode","bankaccno","accname","printcount","losttimes","lang",
            "currency","remark","peoples","mult","prem","amnt","sumprem","dif","paytodate","firstpaydate","cvalidate",
            "inputoperator","inputdate","inputtime","approveflag","approvecode","approvedate","approvetime","uwflag",
            "uwoperator","uwdate","uwtime","appflag","polapplydate","getpoldate","getpoltime","customgetpoldate",
            "state","operator","makedate","maketime","modifydate","modifytime","firsttrialoperator","firsttrialdate",
            "firsttrialtime","receiveoperator","receivedate","receivetime","tempfeeno","proposaltype","salechnldetail",
            "contprintloflag","contpremfeeno","customerreceiptno","cinvalidate","copys","degreetype","handlerdate",
            "handlerprint","stateflag","premscope","intlflag","uwconfirmno","payertype","grpagentcom","grpagentcode",
            "grpagentname","acctype","crs_salechnl","crs_busstype","grpagentidno","duefeemsgflag","paymethod","xpaymode",
            "xbankcode","xbankaccno","xaccname","etl_dt","etl_tm","etl_fg","op_ts","current_ts"};

    public static final String lccontextend_ins = "KLHBASE:LCCONTEXTEND_INS";
    public static final String lccontextend_del = "KLHBASE:LCCONTEXTEND_DEL";
    public static final String[] lccontextend_rowkeys = {"code"};
    public static final String[] lccontextend_columnNames = {"contno","operatesource","back1","back2","back3","back4",
            "policyflag","preformancecom","back5","back6","back7","back8","back9","back10","back11","back12","etl_dt",
            "etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lcinsured_ins = "KLHBASE:LCINSURED_INS";
    public static final String lcinsured_del = "KLHBASE:LCINSURED_DEL";
    public static final String[] lcinsured_rowkeys = {"code"};
    public static final String[] lcinsured_columnNames = {"grpcontno","contno","insuredno","prtno","appntno","managecom",
            "executecom","familyid","relationtomaininsured","relationtoappnt","addressno","sequenceno","name",
            "sex","birthday","idtype","idno","nativeplace","nationality","rgtaddress","marriage","marriagedate",
            "health","stature","avoirdupois","degree","creditgrade","bankcode","bankaccno","accname","joincompanydate",
            "startworkdate","position","salary","occupationtype","occupationcode","worktype","pluralitytype","smokeflag",
            "contplancode","operator","insuredstat","makedate","maketime","modifydate","modifytime","uwflag","uwcode",
            "uwdate","uwtime","bmi","insuredpeoples","contplancount","diskimportno","othidtype","othidno","englishname",
            "grpinsuredphone","medicalinsflag","destination","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lcphoinfonewresult_ins = "KLHBASE:LCPHOINFONEWRESULT_INS";
    public static final String lcphoinfonewresult_del = "KLHBASE:LCPHOINFONEWRESULT_DEL";
    public static final String[] lcphoinfonewresult_rowkeys = {"code"};
    public static final String[] lcphoinfonewresult_columnNames = {"contno","state","sendstate","senddate","standbyflag1",
            "standbyflag2","makedate","maketime","modifydate","modifytime","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lcpol_ins = "KLHBASE:LCPOL_INS";
    public static final String lcpol_del = "KLHBASE:LCPOL_DEL";
    public static final String[] lcpol_rowkeys = {"code"};
    public static final String[] lcpol_columnNames = {};

    public static final String ldcode_ins = "KLHBASE:LDCODE_INS";
    public static final String ldcode_del = "KLHBASE:LDCODE_DEL";
    public static final String[] ldcode_rowkeys = {"code"};
    public static final String[] ldcode_columnNames = {"codetype","code","codename","codealias","comcode","othersign",
            "etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};


    public static final String ldcom_ins = "KLHBASE:LDCOM_INS";
    public static final String ldcom_del = "KLHBASE:LDCOM_DEL";
    public static final String[] ldcom_rowkeys = {"code"};
    public static final String[] ldcom_columnNames = {"comcode","outcomcode","name","shortname","address","zipcode","phone",
            "fax","email","webaddress","satrapname","insumonitorcode","insureid","signid","regionalismcode","comnature",
            "validcode","sign","comcitysize","servicename","serviceno","servicephone","servicepostaddress",
            "servicepostzipcode","letterservicename","letterserviceno","letterservicephone","letterservicepostaddress",
            "letterservicepostzipcode","comgrade","comareatype","innercomname","salecomname","taxregistryno","showname",
            "servicephone2","claimreportphone","peorderphone","backupaddress1","backupaddress2","backupphone1","backupphone2",
            "ename","eshortname","eaddress","eservicepostaddress","eletterservicename","eletterservicepostaddress",
            "ebackupaddress1","ebackupaddress2","servicephone1","printcomname","supercomcode","operator","makedate",
            "maketime","modifydate","modifytime","crs_check_status","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};



    public static final String ldplan_ins = "KLHBASE:LDPLAN_INS";
    public static final String ldplan_del = "KLHBASE:LDPLAN_DEL";
    public static final String[] ldplan_rowkeys = {"code"};
    public static final String[] ldplan_columnNames = {};

    public static final String ljagetendorse_ins = "KLHBASE:LJAGETENDORSE_INS";
    public static final String ljagetendorse_del = "KLHBASE:LJAGETENDORSE_DEL";
    public static final String[] ljagetendorse_rowkeys = {"code"};
    public static final String[] ljagetendorse_columnNames = {};

    public static final String ljapayperson_ins = "KLHBASE:LJAPAYPERSON_INS";
    public static final String ljapayperson_del = "KLHBASE:LJAPAYPERSON_DEL";
    public static final String[] ljapayperson_rowkeys = {"code"};
    public static final String[] ljapayperson_columnNames = {};

    public static final String ljtempfeeclass_ins = "KLHBASE:LJTEMPFEECLASS_INS";
    public static final String ljtempfeeclass_del = "KLHBASE:LJTEMPFEECLASS_DEL";
    public static final String[] ljtempfeeclass_rowkeys = {"code"};
    public static final String[] ljtempfeeclass_columnNames = {};

    public static final String lktransstatus_ins = "KLHBASE:LKTRANSSTATUS_INS";
    public static final String lktransstatus_del = "KLHBASE:LKTRANSSTATUS_DEL";
    public static final String[] lktransstatus_rowkeys = {"code"};
    public static final String[] lktransstatus_columnNames = {};

    public static final String lmedoritem_ins = "KLHBASE:LMEDORITEM_INS";
    public static final String lmedoritem_del = "KLHBASE:LMEDORITEM_DEL";
    public static final String[] lmedoritem_rowkeys = {"code"};
    public static final String[] lmedoritem_columnNames = {"edorcode","edorname","appobj","displayflag","calflag","needdetail","grpneedlist",
            "edorpopedom","edortypeflag","etl_dt","etl_tm","etl_fg","op_ts","current_ts","load_date"};

    public static final String lmriskapp_ins = "KLHBASE:LMRISKAPP_INS";
    public static final String lmriskapp_del = "KLHBASE:LMRISKAPP_DEL";
    public static final String[] lmriskapp_rowkeys = {"code"};
    public static final String[] lmriskapp_columnNames = {};

    public static final String lpedoritem_ins = "KLHBASE:LPEDORITEM_INS";
    public static final String lpedoritem_del = "KLHBASE:LPEDORITEM_DEL";
    public static final String[] lpedoritem_rowkeys = {"code"};
    public static final String[] lpedoritem_columnNames = {};





}
