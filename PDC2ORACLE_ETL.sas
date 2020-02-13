/********************************************************************
*
*	Programmer	:	Joshua Kim
*	Date		:	01.30.2018
*	Program		:	PICAS_TABLE_STRUCTURE
*	Description :	PDC export into proper PICAS DBF formated tables
*	Version		: 	2.4.0
*	Change Log	: 	Updated the user id code 
*
*********************************************************************/

******************************************************************************;
*	Data File Settings
******************************************************************************;

/*
IF FIRST LOAD THEN 
	BATCH_ID = MAX_BATCH_ID + 1
	RUN = 1

ELSE 
	BATCH_ID = BATCH_ID
	RUN = RUN


*/

%LET is_first_load = 1;

%LET batch_id = 1;
%LET run_id = 1;


%LET FLDRDATE = 01172020;

%LET HYGFOLDER = P:\Data_Ops\RVdataforhygiene\&FLDRDATE;

/**** Test Hygiene Folder *****************************************************/
/*%LET HYGFOLDER = C:\Projects\SAS\OUTPUT\&FLDRDATE;*/
/*/******************************************************************************/*/;

%LET USERPATH = P:\Data_Ops\PROCESS FOLDERS\PICAS_TABLE_STRUCTURE\Users;

%LET DATAFILE = DataFile;

******************************************************************************;
*	PDC Settings
******************************************************************************;

%LET MAINDB = PICASDC.MDSOL.COM;
%LET LOCALDB = TS24.MDSOL.COM; 
%LET FPATH = RaveWebServices/DataSets/GetProtocolInfo.csv;

%LET USERNAME = rwsuser;

%LET MAINPW = picasdc1;
%LET LOCALPW = picasdc1;

%LET PRODEN = PROD;
%LET UATEN = UAT;

******************************************************************************;
*	Common Library Settings
******************************************************************************;

%LET MACPATH = P:\Data_Ops\PROCESS FOLDERS\SAS9.4_LIB;

OPTIONS orientation=landscape /*Printing option*/
    mautosource sasautos=("&MACPATH" sasautos )
    mprint mlogic mrecall merror symbolgen;

******************************************************************************;
*	Oracle Database Settings
******************************************************************************;

%CONN2PICAS( HYGIENE, TEST_HYGIENE, welcome );
%CONN2PICAS( STAGING, TEST_STAGING, welcome );

/*%CONN2PICAS( HYGIENE, HYGIENE, welcome );*/
/*%CONN2PICAS( STAGING, STAGING, welcome );*/

/*%ORACLE_CONNECT( */
/*			picas-dqa.ckiyet9acsp0.us-east-1.rds.amazonaws.com,	/* Server URL */*/
/*			1521,		/* Port Number: oracle default 1521*/*/
/*			q003,	/* Service Name */*/
/*			TSM10, 		/* Libname Folder Name */*/
/*			TSM10, 		/* User Id */*/
/*			wel#prev,			/* Password */*/
/*		    ACCESS_TYPE = READONLY /* ( READONLY | WRITE ) */*/
/*			); */

******************************************************************************;
*	End of Settings
******************************************************************************;

******************************************************************************;
*	Start of Macros
******************************************************************************;

%MACRO PDC2SAS( TABLE, COMPANY, PROTOCOL, ENV=&PRODEN, DBPATH=&MAINDB );

FILENAME PDCPULL 
URL "https://&DBPATH./&FPATH.?ProjectName=PDC%NRSTR(&Environment)=&ENV%NRSTR(&SiteName)=&COMPANY.%NRSTR(&SubjectName)=&PROTOCOL"
USER="&USERNAME"
PASS="&MAINPW"
BLOCKSIZE=256
DEBUG
;

%EXTABLE( &TABLE, PDCPULL );	

%MEND PDC2SAS;

******************************************************************************;
*	End of Macros
******************************************************************************;

**********************************************************;
**********************************************************;
******               EXTRACT TABLES                 ******;
**********************************************************;
**********************************************************;

%XLSX2SAS( &HYGFOLDER, datafile, &DATAFILE ); 
/*SHEETNAME=Sheet1 );*/

data pull_list;
	set datafile;
	where company ne ' ' or protocol ne ' ';
run;

data _NULL_	;
	set pull_list;
	tname = cats("PEX" ,_N_);
	call execute( '%PDC2SAS( '||tname||', '||Company||', '||Protocol||' )' );
run;

data cmbtlb;
	set PEX:;
run;

proc sql noprint;		
	select ( case when max( batch_id ) is null then 1 else max( batch_id ) + 1 end ) as NBID
	into :next_batch_id
	from hygiene.qa_status;
quit;

data cmbtlb2;
	set cmbtlb;
	format 
		BATCH_ID 12.
		RUN_ID 12.
		PS_STATUS_ID 4.
		ADD_DATETIME DATETIME.
		;

	%if &is_first_load = 1 %then %do;
			BATCH_ID = &next_batch_id;
			RUN_ID = 1;			
	%end;
	%else %do;
			BATCH_ID = &batch_id;
			RUN_ID = &run_id;	
	%end;

	PS_STATUS_ID = 1;
	ADD_DATETIME = datetime();
run;

**********************************************************;
**********************************************************;
******               TRANSFORM TABLES               ******;
**********************************************************;
**********************************************************;

options set= DBFMISCH BLANKS ;

**********************************************************;
**********************************************************;
******              INDICATI TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_INDICATI ( DSIN );

data indicati_pre;
    set &DSIN;
    keep Code Company Indication P_ind
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    if tabletype = "Indicati";
run;

data indicati;
    retain Code Company Indication P_ind
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    set indicati_pre;
run;

%MEND BUILD_INDICATI;


**********************************************************;
**********************************************************;
******              PAYMENTS TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PAYMENTS ( DSIN );

data payments_pre;
    set &DSIN;
    keep Invcode Code Company Procedure Payment Curr Checked Ptlevel Grantlvl Odcest
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    if tabletype = "Payments";
run;

proc sql;
    create table payments_pre_cntry as
    select a.*, b.cntry 
    from payments_pre as a 
		left join &DSIN as b
    		on a.invcode = b.invcode
    where b.tabletype = "Investig";
quit;

data payments_pre_cntry2;
    drop cntry;
    set payments_pre_cntry;
    if Curr = "USD" then Curr = "USA";
    else if Curr = "EUR" then Curr = "EUR";
    else Curr = cntry;
run;

data payments_cnvt;
    set payments_pre_cntry2 ( rename = ( Payment = v1 Ptlevel = v2 Grantlvl = v3 ));
    Payment = input( v1, 16.0 );
    Ptlevel = input( v2, 16.0 );
    Grantlvl = input( v3, 16.0 );

    drop v1 v2 v3;    
run;

data payments;
    retain Invcode Code Company Procedure Payment Curr Checked Ptlevel Grantlvl Odcest
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    set payments_cnvt;    
run;

%MEND BUILD_PAYMENTS;
%BUILD_PAYMENTS( cmbtlb2);

**********************************************************;
**********************************************************;
******              PROCEDUR TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PROCEDUR ( DSIN );

data procedur_pre;
    set &DSIN;
    keep Code Company Procedure Timesperf Clab Clprice Clname Clcurr Invfreq OptCon
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    if tabletype = "Procedur";
run;

data procedur_cnvt;
    set procedur_pre ( rename = ( timesperf = v1 clprice = v2 invfreq = v3 optcon = v4 ));

	timesperf = input( v1, 16.0 );
    clprice = input( v2, 16.0 );
    invfreq = input( v3, 16.0 );
	optcon = input( v4, 16.0 );

    drop v1 v2 v3 v4;
run;

data procedur;
    retain Code Company Procedure Timesperf Clab Clprice Clname Clcurr Invfreq optcon
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    set procedur_cnvt;    
run;

%MEND BUILD_PROCEDUR;

**********************************************************;
**********************************************************;
******              STUDYLVL TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_STUDYLVL ( DSIN );

data studylvl_pre;
    set &DSIN;
    keep Id Code Company Country Category Cost Currency
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;

    if tabletype = "Studylvl";
    if Currency = "USD" then Currency = "USA";
    else if Currency = "EUR" then Currency = "EUR";
    else Currency = Country;
run;

data studylvl_cnvt;
    set studylvl_pre (rename = (Cost = v1) );
    Cost = input( v1, 10.0 );    
    drop v1;    
run;

data studylvl_cnvt2;
    retain Id Company Code Country Category Cost Currency
	BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    set studylvl_cnvt;    
run;

%MEND BUILD_STUDYLVL;

**********************************************************;
**********************************************************;
******                 CLAB TABLE                   ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_CLAB ( DSIN );

/*CLAB Table NO Data*/

proc sql;
    create table clab_pre 
    ( Company varchar(3), 
      Code varchar(35),  
      Country varchar(3), 
      Procedure varchar(8), 
      Clname varchar(3), 
      Clprice numeric(12,2),
      Clcurr varchar(3),
      Picas varchar(1), 
      Timesperf numeric(10,2),
      Loaded varchar(1), 
      Lumped varchar(1) );
quit;

%MEND BUILD_CLAB;

**********************************************************;
**********************************************************;
******               PROTOCOL TABLE                 ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PROTOCOL ( DSIN );

data protocol_pre;
    set &DSIN;
    keep Code Company C_cntry Family Sameasprot Sitecntry Collsite Country Phase Phase1type
        Dosing Status Age Indays Totconf Hourconf Adminroute Totalvisit Studytype Design 
        Duration Dayorweek Lab Date Map Active Endplan Endactual Projpat Comppat 
        Drug Drugtype Comment Loaded Id Screenpt Contrlpt Complpt Randompt Enrollpt 
        Numgroup Cdesign Ctreatmt Numtreat Screendy 
        T1pre T1treat T1post T2treat T2post T3treat T3post T4treat T4post T5treat T5post            
        T6treat T6post T7treat T7post T8treat T8post T9treat T9post Tatreat Tapost Partial
        T1ext T2ext T3ext T4ext T5ext T6ext T7ext T8ext T9ext Taext
        No_data No_incl Cl_con Cro_con Ph1desc Cro Code2 Extension Sampled Clcpp Clcppcurr Clcppexra Title
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;  
    if tabletype = "Protocol";
run;

data protocol_cnvt;
    set protocol_pre;
    format Date_date date10.
           Endplan_date date10.
           Endactual_date date10.;

    Indays_num = input( Indays, 8.0 );
    Totconf_num = input( Totconf, 6.0 );
    Hourconf_num = input( Hourconf, 6.0 );
    Totalvisit_num = input( Totalvisit, 8.0 );
    Duration_num = input( Duration, 8.0 );
    Date_date = input( Date, MMDDYY10. );
    Endplan_date = input( Endplan, MMDDYY10. );
    Endactual_date = input( Endactual, MMDDYY10. );
    Projpat_num = input( Projpat, 5.0 );
    Comppat_num = input( Comppat, 5.0 );
    Drugtype_num = input( Drugtype, 1.0 );
    Screenpt_num = input( Screenpt, 5.0 );
    Contrlpt_num = input( Contrlpt, 5.0 );
    Complpt_num = input( Complpt, 5.0 );

    Enrollpt_num = input( Enrollpt, 5.0 );
    Numgroup_num = input( Numgroup, 5.0 );
    Numtreat_num = input( Numtreat, 5.0 );
    Screendy_num = input( Screendy, 5.0 );
    T1pre_num = input( T1pre, 5.0 );
    T1treat_num = input( T1treat, 5.0 );
    T1post_num = input( T1post, 5.0 );

    T2treat_num = input( T2treat, 5.0 );
    T2post_num = input( T2post, 5.0 );

    T3treat_num = input( T3treat, 5.0 );
    T3post_num = input( T3post, 5.0 );
    
    T4treat_num = input( T4treat, 5.0 );
    T4post_num = input( T4post, 5.0 );

    T5treat_num = input( T5treat, 5.0 );
    T5post_num = input( T5post, 5.0 );

    T6treat_num = input( T6treat, 5.0 );
    T6post_num = input( T6post, 5.0 );

    T7treat_num = input( T7treat, 5.0 );
    T7post_num = input( T7post, 5.0 );

    T8treat_num = input( T8treat, 5.0 );
    T8post_num = input( T8post, 5.0 );

    T9treat_num = input( T9treat, 5.0 );
    T9post_num = input( T9post, 5.0 );    

    Tatreat_num = input( Tatreat, 5.0 );
    Tapost_num = input( Tapost, 5.0 );

    Clcpp_num = input( Clcpp, 16.0 );
    Clcppexra_num = input( Clcppexra, 16.0 );
    
    drop 
        Indays Totconf Hourconf Totalvisit Duration Date Endplan Endactual
        Projpat Comppat Drugtype  Screenpt Contrlpt Complpt Enrollpt Numgroup
        Numtreat Screendy T1pre T1treat T1post T2treat T2post T3treat T3post
        T4treat T4post T5treat T5post T6treat T6post T7treat T7post T8treat
        T8post T9treat T9post Tatreat Tapost Clcpp Clcppexra title;

    rename 
        Indays_num = Indays
        Totconf_num = Totconf
        Hourconf_num = Hourconf
        Totalvisit_num = Totalvisit
        Duration_num = Duration
        Date_date = Date
        Endplan_date = Endplan
        Endactual_date = Endactual
        Projpat_num = Projpat
        Comppat_num = Comppat
        Drugtype_num = Drugtype
        Screenpt_num = Screenpt
        Contrlpt_num = Contrlpt
        Complpt_num = Complpt

        Enrollpt_num =  Enrollpt
        Numgroup_num =  Numgroup
        Numtreat_num =  Numtreat
        Screendy_num =  Screendy
        T1pre_num =  T1pre
        T1treat_num =  T1treat
        T1post_num = T1post
        T2treat_num =  T2treat
        T2post_num =  T2post
        T3treat_num = T3treat
        T3post_num = T3post
        T4treat_num = T4treat
        T4post_num = T4post
        T5treat_num = T5treat
        T5post_num = T5post
        T6treat_num = T6treat
        T6post_num = T6post
        T7treat_num = T7treat
        T7post_num = T7post
        T8treat_num = T8treat
        T8post_num = T8post
        T9treat_num = T9treat
        T9post_num = T9post
        Tatreat_num = Tatreat
        Tapost_num = Tapost
        Clcpp_num = Clcpp
        Clcppexra_num = Clcppexra;
run;

data protocol_cnvt;
    set protocol_cnvt;    
    if phase ne 'A' then age = 'Z';    
    if country = 'XXX' then
        do;
            country = Sitecntry;
            Sitecntry = 'USA';    
        end;
run;

data protocol_cnvt2;    
    retain Code Company C_cntry Family Sameasprot Sitecntry Collsite Country Phase Phase1type
        Dosing Status Age Indays Totconf Hourconf Adminroute Totalvisit Studytype Design 
        Duration Dayorweek Lab Date Map Active Endplan Endactual Projpat Comppat 
        Drug Drugtype Comment Loaded Id Screenpt Contrlpt Complpt Randompt Enrollpt 
        Numgroup Cdesign Ctreatmt Numtreat Screendy 
        T1pre T1treat T1post T2treat T2post T3treat T3post T4treat T4post T5treat T5post            
        T6treat T6post T7treat T7post T8treat T8post T9treat T9post Tatreat Tapost Partial
        T1ext T2ext T3ext T4ext T5ext T6ext T7ext T8ext T9ext Taext
        No_data No_incl Cl_con Cro_con Ph1desc Cro Code2 Extension Sampled Clcpp Clcppcurr Clcppexra
		Title
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;  
    set protocol_cnvt;
run;

%MEND BUILD_PROTOCOL;

**********************************************************;
**********************************************************;
******               INVESTIG TABLE                 ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_INVESTIG ( DSIN );

data investig_pre;
/*Clean up adminroute and age*/
    set &DSIN;
    keep Invcode Code Company Instit Zipcode State Region Metro Patients Pctpaid 
        Totpayment Grantdate Granttot Labcost Ovrhead Ovrheadbas Ovrheadpct Irbfee Otherfee 
        No_pay No_proc Proc_pct Adjothfee Adjothpct Adjovrpct Adjovrfee Adjbdnpct Adjgrnttot Aff
        Cntry Failpat Failfee Curr Cro Flag Droppat Dropfee Txtinst Incomplete Adjment Adjcode Fixfee 
        Primary Managed Facility Sampled Cpptotpay Cpppay Cppovrhead Cppus Cpvus Cppothfee
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    if tabletype = "Investig";
	if Labcost = ' ' then Labcost = '0';
    if Curr = "USD" THEN Curr = "USA";
    ELSE IF Curr = "EUR" THEN Curr = "EUR";
    ELSE Curr = Cntry;
	
run;

data investig_cnvt;
    set investig_pre;
    format Grantdate_date date10.;

    Patients_num = input( Patients, 5.0 );
    Pctpaid_num = input( Pctpaid, 12.2 );
    Totpayment_num = input( Totpayment, 16.0 );

    Grantdate_date = input( Grantdate, MMDDYY10. );

    Granttot_num = input( Granttot, 16.0 );
    Labcost_num = input( Labcost, 16.0 );
    Ovrhead_num = input( Ovrhead, 16.0 );
    Ovrheadpct_num = input( Ovrheadpct, 12.0 );
    Irbfee_num = input( Irbfee, 16.0 );
    Otherfee_num = input( Otherfee, 16.0 );
    No_pay_num = input( No_pay, 8.0 );
    No_proc_num = input( No_proc, 8.0 );

    Proc_pct_num = input( Proc_pct, 12.0 );
    Adjothfee_num = input( Adjothfee, 12.0 );
    Adjothpct_num = input( Adjothpct, 12.0 );
    Adjovrpct_num = input( Adjovrpct, 12.0 );
    Adjovrfee_num = input( Adjovrfee, 12.0 );
    Adjbdnpct_num = input( Adjbdnpct, 12.0 );
    Adjgrnttot_num = input( Adjgrnttot, 12.0 );

    Failpat_num = input( Failpat, 5.0 );
    Failfee_num = input( Failfee, 16.0 );

    Droppat_num = input( Droppat, 5.0 );
    Dropfee_num = input( Dropfee, 16.0 );

    Adjment_num = input( Adjment, 16.0 );

    Fixfee_num = input( Fixfee, 16.0 );

    Cpptotpay_num = input( Cpptotpay, 16.0 );
    Cpppay_num = input( Cpppay, 16.0 );
    Cppovrhead_num = input( Cppovrhead, 16.0 );
    Cppus_num = input( Cppus, 16.0 );
    Cpvus_num = input( Cpvus, 16.0 );
    Cppothfee_num = input( Cppothfee, 16.0 );

    drop Patients Pctpaid Totpayment Grantdate Granttot 
		 Labcost Ovrhead Ovrheadpct Irbfee Otherfee No_pay
         No_proc Proc_pct Adjothfee Adjothpct Adjovrpct
         Adjovrfee Adjbdnpct Adjgrnttot Failpat Failfee
	     Droppat Dropfee Adjment Fixfee Cpptotpay Cpppay
         Cppovrhead Cppus Cpvus Cppothfee;

    rename Patients_num = Patients 		Pctpaid_num = Pctpaid
    	   Totpayment_num =Totpayment 	Grantdate_date = Grantdate
	       Granttot_num = Granttot 		Labcost_num =  Labcost
    	   Ovrhead_num =  Ovrhead 		Ovrheadpct_num =  Ovrheadpct
    	   Irbfee_num = Irbfee 			Otherfee_num =  Otherfee
    	   No_pay_num = No_pay 			No_proc_num =  No_proc
		   Proc_pct_num = Proc_pct 		Adjothfee_num = Adjothfee
    	   Adjothpct_num =  Adjothpct 	Adjovrpct_num =  Adjovrpct
    	   Adjovrfee_num =  Adjovrfee	Adjbdnpct_num = Adjbdnpct
    	   Adjgrnttot_num =  Adjgrnttot Failpat_num = Failpat
    	   Failfee_num =  Failfee	    Droppat_num =  Droppat
    	   Dropfee_num = Dropfee	    Adjment_num =  Adjment
    	   Fixfee_num =  Fixfee		    Cpptotpay_num =  Cpptotpay
    	   Cpppay_num = Cpppay		    Cppovrhead_num =  Cppovrhead
    	   Cppus_num = Cppus		    Cpvus_num = Cpvus
    	   Cppothfee_num =  Cppothfee;
run;

/*fix cpppay it's blank so it's convert to 9999999999*/

data investig_cnvt2;
    retain Invcode Code Company Instit Zipcode State Region Metro Patients Pctpaid 
        Totpayment Grantdate Granttot Labcost Ovrhead Ovrheadbas Ovrheadpct Irbfee Otherfee 
        No_pay No_proc Proc_pct Adjothfee Adjothpct Adjovrpct Adjovrfee Adjbdnpct Adjgrnttot Aff
        Cntry Failpat Failfee Curr Cro Flag Droppat Dropfee Txtinst Incomplete Adjment Adjcode Fixfee 
        Primary Managed Facility Sampled Cpptotpay Cpppay Cppovrhead Cppus Cpvus Cppothfee
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    set investig_cnvt;
run;

%MEND BUILD_INVESTIG;

**********************************************************;
**********************************************************;
******               QA_STATUS TABLE                ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_QASTATUS ( DSIN );

data qastatus_pre;
/*replace user*/
    set &DSIN;
    keep Code Company Country Sitecntry Coll_Site Status Comment C_date 
        Inven_user Inven_date Inven_log Inven_redo 
        Med_user Med_date Med_log Med_redo 
        Clab_user Clab_date Clab_log Clab_redo 
        Invst_user Invst_date Invst_log Invst_redo
        Psper_user Psper_date Clab
        Isper_user Isper_date 
        Addition
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;
    if tabletype = "QA Status";
run;

data qastatus_cnvt;
	drop Sitecntry;
    set qastatus_pre;

    format C_date_date date10.
           Inven_date_date date10.
           Med_date_date date10.
           Clab_date_date date10.
           Invst_date_date date10.
           Psper_date_date date10.
           Isper_date_date date10.;
           
	if country = . then country = sitecntry;

    Status_num = input( Status, 4.0 );
    C_date_date = input( C_date, MMDDYY10. );
    
	Inven_date_date = input( trim(Inven_date), MMDDYY10. );
    Inven_log_num = input( Inven_log, 11.0 );
    
	Med_date_date = input( trim(Med_date), MMDDYY10. );
    Med_log_num = input( Med_log, 11.0 );

	Clab_date_date = input( trim(Clab_date), MMDDYY10. );
    Clab_log_num = input( Clab_log, 11.0 );

	Invst_date_date = input( trim(Invst_date), MMDDYY10. );
    Invst_log_num = input( Invst_log, 11.0 );
    
    Psper_date_date = input( trim(Psper_date), MMDDYY10. );

    Isper_date_date = input( trim(Isper_date), MMDDYY10. );

    drop Status C_date 
          Inven_date Inven_log
          Med_date Med_log
          Clab_date Clab_log
          Invst_date Invst_log    
          Psper_date    
          Isper_date;

    rename Status_num = Status 
		   C_date_date = C_date
           
           Inven_date_date = Inven_date
           Inven_log_num = Inven_log

           Med_date_date = Med_date
           Med_log_num = Med_log

           
           Clab_date_date = Clab_date
           Clab_log_num = Clab_log
    
           
           Invst_date_date = Invst_date
           Invst_log_num = Invst_log
        
           
           Psper_date_date = Psper_date
    
           
           Isper_date_date = Isper_date;
run;

data qastatus_cnvt2;
    retain Company Code Country Coll_site Status Comment C_date 
        Inven_user Inven_date Inven_log Inven_redo
        Med_user Med_date Med_log Med_redo
        Clab_user Clab_date Clab_log Clab_redo
        Invst_user Invst_date Invst_log Invst_redo
        Psper_user Psper_date clab
        Isper_user Isper_date addition
		BATCH_ID RUN_ID PS_STATUS_ID ADD_DATETIME;;
    set qastatus_cnvt;    
run;

%MEND BUILD_QASTATUS;

**********************************************************;
**********************************************************;
******               CLEAN TABLES                   ******;
**********************************************************;
**********************************************************;

**********************************************************;
**********************************************************;
******               LOAD TABLES                    ******;
**********************************************************;
**********************************************************;






























































**********************************************************;
**********************************************************;
******                IMPORT TABLE                  ******;
**********************************************************;
**********************************************************;


options set= DBFMISCH BLANKS ;
**********************************************************;
**********************************************************;
******              INDICATI TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_INDICATI ( SOURCE, OUTPUT );

data indicati_pre;
    set &SOURCE;
    keep Code Company Indication P_ind;
    if tabletype = "Indicati";
run;

data indicati_pre;
    retain Code Company Indication P_ind;
    set indicati_pre;
run;

proc dbload
    dbms = dbf
    data = indicati_pre;  
    path = "&OUTPUT.\INDICATI.dbf";
    type Code = 'char(35)'
         Company = 'char(3)'
         Indication = 'char(10)'
         P_ind = logical;
    limit = 0;
    load; 
run;

%MEND BUILD_INDICATI;

**********************************************************;
**********************************************************;
******              PAYMENTS TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PAYMENTS ( SOURCE, OUTPUT );

data payments_pre;
    set &SOURCE;
    keep Invcode Code Company Procedure Payment Curr Checked Ptlevel Grantlvl Odcest;
    if tabletype = "Payments";
run;

proc sql;
    create table payments_pre_cntry as
    select a.*, b.cntry 
    from payments_pre as a left join &SOURCE as b
    on a.invcode = b.invcode
    where b.tabletype = "Investig";
quit;

data payments_pre_cntry2;
    drop cntry;
    set payments_pre_cntry;
    if Curr = "USD" THEN Curr = "USA";
    ELSE IF Curr = "EUR" THEN Curr = "EUR";
    ELSE Curr = cntry;
run;

data payments_cnvt;
    set payments_pre_cntry2;
    Payment_num = input( Payment, 16.0 );
    Ptlevel_num = input( Ptlevel, 16.0 );
    Grantlvl_num = input( Grantlvl, 16.0 );
    drop payment Ptlevel Grantlvl cntry;
    rename Payment_num = Payment Ptlevel_num = Ptlevel Grantlvl_num = Grantlvl;
run;

data payments_cnvt2;
    retain Invcode Code Company Procedure Payment Curr Checked Ptlevel Grantlvl Odcest;
    set payments_cnvt;    
run;

proc dbload
    dbms = dbf
    data = payments_cnvt2;  
    path="&OUTPUT.\PAYMENTS.dbf";
    type Invcode = 'char(35)'
         Code = 'char(35)'
         Company = 'char(3)'
         Procedure = 'char(8)'
         Payment = 'numeric(16,2)'
         Curr = 'char(3)'
         Checked = 'char(10)'
         Ptlevel = 'numeric(16,2)'
         Grantlvl = 'numeric(16,2)'
         Odcest = logical;
    limit=0;
    load; 
run;

%MEND BUILD_PAYMENTS;

**********************************************************;
**********************************************************;
******              PROCEDUR TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PROCEDUR ( SOURCE, OUTPUT );

data procedur_pre;
    set &SOURCE;
    keep Code Company Procedure Timesperf Clab Clprice Clname Clcurr Invfreq OptCon;
    if tabletype = "Procedur";
run;

data procedur_cnvt;
    set procedur_pre;
    timesperf_num = input( timesperf, 16.0 );
    clprice_num = input( clprice, 16.0 );
    invfreq_num = input( invfreq, 16.0 );
	optcon_num = input( optcon, 16.0 );
    drop timesperf clprice invfreq optcon;
    rename timesperf_num = timesperf clprice_num = clprice invfreq_num = invfreq
			optcon_num = optcon;
run;

data procedur_cnvt2;
    retain Code Company Procedure Timesperf Clab Clprice Clname Clcurr Invfreq optcon;
    set procedur_cnvt;    
run;

data procedure_optcon;
	set procedur_cnvt2;
	where optcon > 0;
run;

data procedur_cnvt2;
    drop optcon;
    set procedur_cnvt2;    
	if optcon > 0 and timesperf = 0 and invfreq = 0 then delete;
run;

proc dbload
    dbms = dbf
    data = procedur_cnvt2;  
    path="&OUTPUT.\PROCEDUR.dbf";
    type Code = 'char(35)'
         Company = 'char(3)'
         Procedure = 'char(8)'
         Timesperf = 'numeric(10,2)'
         Clab = logical
         Clprice = 'numeric(12,2)'
         Clname = 'char(3)'
         Clcurr = 'char(3)'
         Invfreq = 'numeric(8,2)'
/*		 Optcon = 'numeric(10,2)';*/
		 ;
    limit=0;
    load; 
run;

proc dbload
    dbms = dbf
    data = procedure_optcon;  
    path="&OUTPUT.\PROCEDUR_OPTCON.dbf";
    type Code = 'char(35)'
         Company = 'char(3)'
         Procedure = 'char(8)'
         Timesperf = 'numeric(10,2)'
         Clab = logical
         Clprice = 'numeric(12,2)'
         Clname = 'char(3)'
         Clcurr = 'char(3)'
         Invfreq = 'numeric(8,2)'
		 Optcon = 'numeric(10,2)';
		 ;
    limit=0;
    load; 
run;

%MEND BUILD_PROCEDUR;

**********************************************************;
**********************************************************;
******              STUDYLVL TABLE                  ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_STUDYLVL ( SOURCE, OUTPUT );

data studylvl_pre;
    set &SOURCE;
    keep Id Code Company Country Category Cost Currency;
    if tabletype = "Studylvl";
    if Currency = "USD" THEN Currency = "USA";
    ELSE IF Currency = "EUR" THEN Currency = "EUR";
    ELSE Currency = Country;
run;

data studylvl_cnvt;
    set studylvl_pre;
    Cost_num = input( Cost, 10.0 );    
    drop Cost;
    rename Cost_num = Cost;
run;

data studylvl_cnvt2;
    retain Id Company Code Country Category Cost Currency;
    set studylvl_cnvt;    
run;

proc dbload
    dbms = dbf
    data = studylvl_cnvt2;  
    path="&OUTPUT.\STUDYLVL.dbf";
    type Id = 'char(8)'
         Company = 'char(3)'
         Code = 'char(35)'
         Country = 'char(8)'
         Category = 'char(5)'
         Cost = 'numeric(10,2)'
         Currency = 'char(9)';
    limit=0;
    load; 
run;

%MEND BUILD_STUDYLVL;

**********************************************************;
**********************************************************;
******                 CLAB TABLE                   ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_CLAB ( SOURCE, OUTPUT );

/*CLAB Table NO Data*/

proc sql;
    create table clab_pre 
    ( Company varchar(3), 
      Code varchar(35),  
      Country varchar(3), 
      Procedure varchar(8), 
      Clname varchar(3), 
      Clprice numeric(12,2),
      Clcurr varchar(3),
      Picas varchar(1), 
      Timesperf numeric(10,2),
      Loaded varchar(1), 
      Lumped varchar(1) );
quit;

proc dbload
    dbms = dbf
    data = clab_pre;  
    path="&OUTPUT.\CLAB.dbf";
    type Company = 'char(3)'
         Code = 'char(35)' 
         Country = 'char(3)' 
         Procedure = 'char(8)' 
         Clname = 'char(3)' 
         Clprice = 'numeric(12,2)'
         Clcurr = 'char(3)'
         Picas = logical
         Timesperf = 'numeric(10,2)'
         Loaded = 'char(1)'
         Lumped = logical;
    limit=0;
    load; 
run;

%MEND BUILD_CLAB;

**********************************************************;
**********************************************************;
******               PROTOCOL TABLE                 ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_PROTOCOL ( SOURCE, OUTPUT );

data protocol_pre;
    set &SOURCE;
    keep Code Company C_cntry Family Sameasprot Sitecntry Collsite Country Phase Phase1type
        Dosing Status Age Indays Totconf Hourconf Adminroute Totalvisit Studytype Design 
        Duration Dayorweek Lab Date Map Active Endplan Endactual Projpat Comppat 
        Drug Drugtype Comment Loaded Id Screenpt Contrlpt Complpt Randompt Enrollpt 
        Numgroup Cdesign Ctreatmt Numtreat Screendy 
        T1pre T1treat T1post T2treat T2post T3treat T3post T4treat T4post T5treat T5post            
        T6treat T6post T7treat T7post T8treat T8post T9treat T9post Tatreat Tapost Partial
        T1ext T2ext T3ext T4ext T5ext T6ext T7ext T8ext T9ext Taext
        No_data No_incl Cl_con Cro_con Ph1desc Cro Code2 Extension Sampled Clcpp Clcppcurr Clcppexra Title;  
    if tabletype = "Protocol";
run;

data protocol_cnvt;
    set protocol_pre;
    format Date_date date10.
           Endplan_date date10.
           Endactual_date date10.;

    Indays_num = input( Indays, 8.0 );
    Totconf_num = input( Totconf, 6.0 );
    Hourconf_num = input( Hourconf, 6.0 );
    Totalvisit_num = input( Totalvisit, 8.0 );
    Duration_num = input( Duration, 8.0 );
    Date_date = input( Date, MMDDYY10. );
    Endplan_date = input( Endplan, MMDDYY10. );
    Endactual_date = input( Endactual, MMDDYY10. );
    Projpat_num = input( Projpat, 5.0 );
    Comppat_num = input( Comppat, 5.0 );
    Drugtype_num = input( Drugtype, 1.0 );
    Screenpt_num = input( Screenpt, 5.0 );
    Contrlpt_num = input( Contrlpt, 5.0 );
    Complpt_num = input( Complpt, 5.0 );

    Enrollpt_num = input( Enrollpt, 5.0 );
    Numgroup_num = input( Numgroup, 5.0 );
    Numtreat_num = input( Numtreat, 5.0 );
    Screendy_num = input( Screendy, 5.0 );
    T1pre_num = input( T1pre, 5.0 );
    T1treat_num = input( T1treat, 5.0 );
    T1post_num = input( T1post, 5.0 );

    T2treat_num = input( T2treat, 5.0 );
    T2post_num = input( T2post, 5.0 );

    T3treat_num = input( T3treat, 5.0 );
    T3post_num = input( T3post, 5.0 );
    
    T4treat_num = input( T4treat, 5.0 );
    T4post_num = input( T4post, 5.0 );

    T5treat_num = input( T5treat, 5.0 );
    T5post_num = input( T5post, 5.0 );

    T6treat_num = input( T6treat, 5.0 );
    T6post_num = input( T6post, 5.0 );

    T7treat_num = input( T7treat, 5.0 );
    T7post_num = input( T7post, 5.0 );

    T8treat_num = input( T8treat, 5.0 );
    T8post_num = input( T8post, 5.0 );

    T9treat_num = input( T9treat, 5.0 );
    T9post_num = input( T9post, 5.0 );    

    Tatreat_num = input( Tatreat, 5.0 );
    Tapost_num = input( Tapost, 5.0 );

    Clcpp_num = input( Clcpp, 16.0 );
    Clcppexra_num = input( Clcppexra, 16.0 );
    
    drop 
        Indays Totconf Hourconf Totalvisit Duration Date Endplan Endactual
        Projpat Comppat Drugtype  Screenpt Contrlpt Complpt Enrollpt Numgroup
        Numtreat Screendy T1pre T1treat T1post T2treat T2post T3treat T3post
        T4treat T4post T5treat T5post T6treat T6post T7treat T7post T8treat
        T8post T9treat T9post Tatreat Tapost Clcpp Clcppexra title;

    rename 
        Indays_num = Indays
        Totconf_num = Totconf
        Hourconf_num = Hourconf
        Totalvisit_num = Totalvisit
        Duration_num = Duration
        Date_date = Date
        Endplan_date = Endplan
        Endactual_date = Endactual
        Projpat_num = Projpat
        Comppat_num = Comppat
        Drugtype_num = Drugtype
        Screenpt_num = Screenpt
        Contrlpt_num = Contrlpt
        Complpt_num = Complpt

        Enrollpt_num =  Enrollpt
        Numgroup_num =  Numgroup
        Numtreat_num =  Numtreat
        Screendy_num =  Screendy
        T1pre_num =  T1pre
        T1treat_num =  T1treat
        T1post_num = T1post
        T2treat_num =  T2treat
        T2post_num =  T2post
        T3treat_num = T3treat
        T3post_num = T3post
        T4treat_num = T4treat
        T4post_num = T4post
        T5treat_num = T5treat
        T5post_num = T5post
        T6treat_num = T6treat
        T6post_num = T6post
        T7treat_num = T7treat
        T7post_num = T7post
        T8treat_num = T8treat
        T8post_num = T8post
        T9treat_num = T9treat
        T9post_num = T9post
        Tatreat_num = Tatreat
        Tapost_num = Tapost
        Clcpp_num = Clcpp
        Clcppexra_num = Clcppexra;
run;

data protocol_cnvt;
    set protocol_cnvt;    
    if phase ne 'A' then age = 'Z';    
    if country = 'XXX' then
        do;
            country = Sitecntry;
            Sitecntry = 'USA';    
        end;
run;

data protocol_cnvt2;    
    retain Code Company C_cntry Family Sameasprot Sitecntry Collsite Country Phase Phase1type
        Dosing Status Age Indays Totconf Hourconf Adminroute Totalvisit Studytype Design 
        Duration Dayorweek Lab Date Map Active Endplan Endactual Projpat Comppat 
        Drug Drugtype Comment Loaded Id Screenpt Contrlpt Complpt Randompt Enrollpt 
        Numgroup Cdesign Ctreatmt Numtreat Screendy 
        T1pre T1treat T1post T2treat T2post T3treat T3post T4treat T4post T5treat T5post            
        T6treat T6post T7treat T7post T8treat T8post T9treat T9post Tatreat Tapost Partial
        T1ext T2ext T3ext T4ext T5ext T6ext T7ext T8ext T9ext Taext
        No_data No_incl Cl_con Cro_con Ph1desc Cro Code2 Extension Sampled Clcpp Clcppcurr Clcppexra;  
    set protocol_cnvt;
run;

data protocol_title;
    keep company code title;
    set protocol_pre;
run;

proc dbload
    dbms = dbf
    data = protocol_cnvt2;  
    path="&OUTPUT.\PROTOCOL.dbf";
    type Code = 'char(35)' 
         Company = 'char(3)'
         C_cntry = 'char(3)'
         Family = 'char(35)' 
         Sameasprot = logical
         Sitecntry = 'char(3)'
         Collsite = 'char(1)'
         Country = 'char(3)' 
         Phase = 'char(1)'
         Phase1type = 'char(2)'
         Dosing = 'char(1)'
         Status = 'char(1)'
         Age = 'char(1)'
         Indays = 'numeric(8,0)'
         Totconf = 'numeric(6,0)'
         Hourconf = 'numeric(6,0)'
         Adminroute = 'char(1)'
         Totalvisit = 'numeric(8,0)'
         Studytype = 'char(1)'
         Design = 'char(2)'
         Duration = 'numeric(4,0)'
         Dayorweek = 'char(1)'
         Lab = logical
         Date = 'date'
         Map = 'char(20)'
         Active = logical
         Endplan = 'date'
         Endactual = 'date'
         Projpat = 'numeric(5,0)'
         Comppat = 'numeric(5,0)'
         Drug = 'char(50)'
         Drugtype = 'numeric(1,0)'
         Comment = 'char(250)'
         Loaded = 'char(1)'
         Id = 'char(8)'
         Screenpt = 'numeric(5,0)'
         Contrlpt = 'numeric(5,0)'
         Complpt = 'numeric(5,0)'
         Randompt = 'char(1)'
         Enrollpt = 'numeric(5,0)'
         Numgroup = 'numeric(5,0)'
         Cdesign = 'char(2)'
         Ctreatmt = 'char(2)'
         Numtreat = 'numeric(5,0)'
         Screendy = 'numeric(5,0)'
         T1pre = 'numeric(5,0)'
         T1treat = 'numeric(5,0)'
         T1post = 'numeric(5,0)'
         T2treat = 'numeric(5,0)'
         T2post = 'numeric(5,0)'
         T3treat = 'numeric(5,0)'
         T3post = 'numeric(5,0)'
         T4treat = 'numeric(5,0)'
         T4post = 'numeric(5,0)'
         T5treat = 'numeric(5,0)'
         T5post = 'numeric(5,0)'
         T6treat = 'numeric(5,0)'
         T6post = 'numeric(5,0)'
         T7treat = 'numeric(5,0)'
         T7post = 'numeric(5,0)'
         T8treat = 'numeric(5,0)'
         T8post = 'numeric(5,0)'
         T9treat = 'numeric(5,0)'
         T9post = 'numeric(5,0)'
         Tatreat = 'numeric(5,0)'
         Tapost = 'numeric(5,0)'
         Partial = logical         
         T1ext = logical         
         T2ext = logical
         T3ext = logical
         T4ext = logical
         T5ext = logical
         T6ext = logical
         T7ext = logical
         T8ext = logical
         T9ext = logical
         Taext = logical
         No_data = logical
         No_incl = logical
         Cl_con = logical
         Cro_con = logical
         Ph1desc = 'char(10)'
         Cro = logical
         Code2 = 'char(35)'
         Extension = logical
         Sampled = logical
         Clcpp = 'numeric(16,2)'
         Clcppcurr = 'char(3)'
         Clcppexra = 'numeric(16,4)';
    limit=0;
    load; 
run;

proc export
    data = protocol_title
    outfile = "&OUTPUT.\PROTOCOL_FPT"
    dbms = xlsx
    replace;
    sheet = "PRT_TITLE";
run;

%MEND BUILD_PROTOCOL;

**********************************************************;
**********************************************************;
******               INVESTIG TABLE                 ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_INVESTIG ( SOURCE, OUTPUT );

data investig_pre;
/*Clean up adminroute and age*/
    set &SOURCE;
    keep Invcode Code Company Instit Zipcode State Region Metro Patients Pctpaid 
        Totpayment Grantdate Granttot Labcost Ovrhead Ovrheadbas Ovrheadpct Irbfee Otherfee 
        No_pay No_proc Proc_pct Adjothfee Adjothpct Adjovrpct Adjovrfee Adjbdnpct Adjgrnttot Aff
        Cntry Failpat Failfee Curr Cro Flag Droppat Dropfee Txtinst Incomplete Adjment Adjcode Fixfee 
        Primary Managed Facility Sampled Cpptotpay Cpppay Cppovrhead Cppus Cpvus Cppothfee;
    if tabletype = "Investig";
	if Labcost = ' ' then Labcost = '0';
    if Curr = "USD" THEN Curr = "USA";
    ELSE IF Curr = "EUR" THEN Curr = "EUR";
    ELSE Curr = Cntry;
	
run;

data investig_cnvt;
    set investig_pre;
    format Grantdate_date date10.;

    Patients_num = input( Patients, 5.0 );
    Pctpaid_num = input( Pctpaid, 12.2 );
    Totpayment_num = input( Totpayment, 16.0 );

    Grantdate_date = input( Grantdate, MMDDYY10. );

    Granttot_num = input( Granttot, 16.0 );
    Labcost_num = input( Labcost, 16.0 );
    Ovrhead_num = input( Ovrhead, 16.0 );
    Ovrheadpct_num = input( Ovrheadpct, 12.0 );
    Irbfee_num = input( Irbfee, 16.0 );
    Otherfee_num = input( Otherfee, 16.0 );
    No_pay_num = input( No_pay, 8.0 );
    No_proc_num = input( No_proc, 8.0 );

    Proc_pct_num = input( Proc_pct, 12.0 );
    Adjothfee_num = input( Adjothfee, 12.0 );
    Adjothpct_num = input( Adjothpct, 12.0 );
    Adjovrpct_num = input( Adjovrpct, 12.0 );
    Adjovrfee_num = input( Adjovrfee, 12.0 );
    Adjbdnpct_num = input( Adjbdnpct, 12.0 );
    Adjgrnttot_num = input( Adjgrnttot, 12.0 );

    Failpat_num = input( Failpat, 5.0 );
    Failfee_num = input( Failfee, 16.0 );

    Droppat_num = input( Droppat, 5.0 );
    Dropfee_num = input( Dropfee, 16.0 );

    Adjment_num = input( Adjment, 16.0 );

    Fixfee_num = input( Fixfee, 16.0 );

    Cpptotpay_num = input( Cpptotpay, 16.0 );
    Cpppay_num = input( Cpppay, 16.0 );
    Cppovrhead_num = input( Cppovrhead, 16.0 );
    Cppus_num = input( Cppus, 16.0 );
    Cpvus_num = input( Cpvus, 16.0 );
    Cppothfee_num = input( Cppothfee, 16.0 );

    drop Patients Pctpaid Totpayment Grantdate Granttot 
		 Labcost Ovrhead Ovrheadpct Irbfee Otherfee No_pay
         No_proc Proc_pct Adjothfee Adjothpct Adjovrpct
         Adjovrfee Adjbdnpct Adjgrnttot Failpat Failfee
	     Droppat Dropfee Adjment Fixfee Cpptotpay Cpppay
         Cppovrhead Cppus Cpvus Cppothfee;

    rename Patients_num = Patients 		Pctpaid_num = Pctpaid
    	   Totpayment_num =Totpayment 	Grantdate_date = Grantdate
	       Granttot_num = Granttot 		Labcost_num =  Labcost
    	   Ovrhead_num =  Ovrhead 		Ovrheadpct_num =  Ovrheadpct
    	   Irbfee_num = Irbfee 			Otherfee_num =  Otherfee
    	   No_pay_num = No_pay 			No_proc_num =  No_proc
		   Proc_pct_num = Proc_pct 		Adjothfee_num = Adjothfee
    	   Adjothpct_num =  Adjothpct 	Adjovrpct_num =  Adjovrpct
    	   Adjovrfee_num =  Adjovrfee	Adjbdnpct_num = Adjbdnpct
    	   Adjgrnttot_num =  Adjgrnttot Failpat_num = Failpat
    	   Failfee_num =  Failfee	    Droppat_num =  Droppat
    	   Dropfee_num = Dropfee	    Adjment_num =  Adjment
    	   Fixfee_num =  Fixfee		    Cpptotpay_num =  Cpptotpay
    	   Cpppay_num = Cpppay		    Cppovrhead_num =  Cppovrhead
    	   Cppus_num = Cppus		    Cpvus_num = Cpvus
    	   Cppothfee_num =  Cppothfee;
run;

/*fix cpppay it's blank so it's convert to 9999999999*/

data investig_cnvt2;
    retain Invcode Code Company Instit Zipcode State Region Metro Patients Pctpaid 
        Totpayment Grantdate Granttot Labcost Ovrhead Ovrheadbas Ovrheadpct Irbfee Otherfee 
        No_pay No_proc Proc_pct Adjothfee Adjothpct Adjovrpct Adjovrfee Adjbdnpct Adjgrnttot Aff
        Cntry Failpat Failfee Curr Cro Flag Droppat Dropfee Txtinst Incomplete Adjment Adjcode Fixfee 
        Primary Managed Facility Sampled Cpptotpay Cpppay Cppovrhead Cppus Cpvus Cppothfee;
    set investig_cnvt;
run;

proc dbload
    dbms = dbf
    data = investig_cnvt2;  
    path="&OUTPUT.\INVESTIG.dbf";
    type Invcode = 'char(35)'
         Code = 'char(35)'
         Company = 'char(3)'
         Instit = 'char(8)'
         Zipcode = 'char(10)'
         State = 'char(2)'
         Region = 'char(7)'
         Metro = 'char(10)'
         Patients = 'numeric(5,0)'
         Pctpaid = 'numeric(12,2)'
         Totpayment = 'numeric(16,2)'
         Grantdate = 'date'
         Granttot = 'numeric(16,2)'
         Labcost = 'numeric(16,2)'
         Ovrhead = 'numeric(16,2)'
         Ovrheadbas = 'char(1)'
         Ovrheadpct = 'numeric(12,2)'
         Irbfee = 'numeric(16,2)'
         Otherfee = 'numeric(16,2)'
         No_pay = 'numeric(8,0)'
         No_proc = 'numeric(8,0)'
         Proc_pct = 'numeric(12,2)'
         Adjothfee = 'numeric(12,2)'
         Adjothpct = 'numeric(12,2)'
         Adjovrpct = 'numeric(12,2)'
         Adjovrfee = 'numeric(12,2)'
         Adjbdnpct = 'numeric(12,2)'
         Adjgrnttot = 'numeric(12,2)'
         Aff = 'char(1)'
         Cntry = 'char(3)'
         Failpat = 'numeric(5,0)'
         Failfee = 'numeric(16,2)'
         Curr = 'char(3)'
         Cro = logical
         Flag = logical
         Droppat = 'numeric(5,0)'
         Dropfee = 'numeric(16,2)'
         Txtinst = 'char(50)'
         Incomplete = logical
         Adjment = 'numeric(16,2)'
         Adjcode = 'char(1)'
         Fixfee = 'numeric(16,2)'
         Primary = logical
         Managed = logical
         Facility = 'char(1)'
         Sampled = logical
         Cpptotpay = 'numeric(16,2)'
         Cpppay = 'numeric(16,2)'
         Cppovrhead = 'numeric(16,2)'
         Cppus = 'numeric(16,2)'
         Cpvus = 'numeric(16,2)'
         Cppothfee = 'numeric(16,2)';
    limit=0;
    load; 
run;

%MEND BUILD_INVESTIG;

**********************************************************;
**********************************************************;
******               QA_STATUS TABLE                ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_QASTATUS ( SOURCE, OUTPUT );

data qastatus_pre;
/*replace user*/
    set &SOURCE;
    keep Code Company Country Sitecntry Coll_Site Status Comment C_date 
        Inven_user Inven_date Inven_log Inven_redo 
        Med_user Med_date Med_log Med_redo 
        Clab_user Clab_date Clab_log Clab_redo 
        Invst_user Invst_date Invst_log Invst_redo
        Psper_user Psper_date Clab
        Isper_user Isper_date 
        Addition;
    if tabletype = "QA Status";
run;

%DBF2SAS( &USERPATH, USERS );

proc sql;
	create table qastatus_pre2 as
	select a.*, 
			b.ID as Inven_user_num label = 'Inven_user_num' format 4.0,
			c.ID as Med_user_num label = 'Med_user_num' format 4.0, 
			d.ID as Clab_user_num label = 'Clab_user_num' format 4.0, 
			e.ID as Invst_user_num label = 'Invst_user_num' format 4.0, 
			f.ID as Psper_user_num label = 'Psper_user_num' format 4.0,
			g.ID as Isper_user_num label = 'Isper_user_num' format 4.0
	from qastatus_pre as a
		left join users as b on upper( strip( a.Inven_user ) ) = upper( strip( b.name ) )
		left join users as c on upper( strip( a.Med_user ) ) = upper( strip( c.name ) )
		left join users as d on upper( strip( a.Clab_user ) ) = upper( strip( d.name ) )
		left join users as e on upper( strip( a.Invst_user ) ) = upper( strip( e.name ) )
		left join users as f on upper( strip( a.Psper_user ) ) = upper( strip( f.name ) )
		left join users as g on upper( strip( a.Isper_user ) ) = upper( strip( g.name ) );		
quit;

data qastatus_cnvt;
	drop Sitecntry;
    set qastatus_pre2;

    format C_date_date date10.
           Inven_date_date date10.
           Med_date_date date10.
           Clab_date_date date10.
           Invst_date_date date10.
           Psper_date_date date10.
           Isper_date_date date10.;
           
	if country = . then country = sitecntry;

    Status_num = input( Status, 4.0 );
    C_date_date = input( C_date, MMDDYY10. );
    
	Inven_date_date = input( trim(Inven_date), MMDDYY10. );
    Inven_log_num = input( Inven_log, 11.0 );
    
	Med_date_date = input( trim(Med_date), MMDDYY10. );
    Med_log_num = input( Med_log, 11.0 );

	Clab_date_date = input( trim(Clab_date), MMDDYY10. );
    Clab_log_num = input( Clab_log, 11.0 );

	Invst_date_date = input( trim(Invst_date), MMDDYY10. );
    Invst_log_num = input( Invst_log, 11.0 );
    
    Psper_date_date = input( trim(Psper_date), MMDDYY10. );

    Isper_date_date = input( trim(Isper_date), MMDDYY10. );

    drop Status C_date 
          Inven_date Inven_log
          Med_date Med_log
          Clab_date Clab_log
          Invst_date Invst_log    
          Psper_date    
          Isper_date;

    rename Status_num = Status C_date_date = C_date
           
           Inven_date_date = Inven_date
           Inven_log_num = Inven_log

           
           Med_date_date = Med_date
           Med_log_num = Med_log

           
           Clab_date_date = Clab_date
           Clab_log_num = Clab_log
    
           
           Invst_date_date = Invst_date
           Invst_log_num = Invst_log
        
           
           Psper_date_date = Psper_date
    
           
           Isper_date_date = Isper_date;
run;

data qastatus_cnvt2;
    retain Company Code Country Coll_site Status Comment C_date 
        Inven_user Inven_date Inven_log Inven_redo
        Med_user Med_date Med_log Med_redo
        Clab_user Clab_date Clab_log Clab_redo
        Invst_user Invst_date Invst_log Invst_redo
        Psper_user Psper_date clab
        Isper_user Isper_date addition;
    set qastatus_cnvt;    
run;

proc dbload
    dbms = dbf
    data = qastatus_cnvt2;  
    path="&OUTPUT.\QA_STATUS.dbf";
    /* 29 Fields */
    type Company = 'char(3)'
         Code = 'char(35)'
         Country = 'char(3)'
         Coll_site = 'char(1)'   
         Status = 'numeric(4,0)'
         Comment = 'char(250)'   
         C_date = 'date' /*Date 8*/

         Inven_user = 'numeric(4,0)'
         Inven_date = 'date'  
         Inven_log = 'numeric(11,0)'
         Inven_redo = logical

         Med_user = 'numeric(4,0)' 
         Med_date = 'date' 
         Med_log =  'numeric(11,0)'
         Med_redo = logical

         Clab_user = 'numeric(4,0)' 
         Clab_date = 'date' 
         Clab_log =  'numeric(11,0)'
         Clab_redo = logical

         Invst_user = 'numeric(4,0)' 
         Invst_date = 'date' 
         Invst_log =  'numeric(11,0)'
         Invst_redo = logical

         Psper_user = 'numeric(4,0)' 
         Psper_date = 'date' 

         Clab = logical

         Isper_user = 'numeric(4,0)' 
         Isper_date = 'date' 

         Addition = logical;       
       ;
    limit=0;
    load; 
run;

%MEND BUILD_QASTATUS;

**********************************************************;
**********************************************************;
******             END OF MACRO TABLE               ******;
**********************************************************;
**********************************************************;

%MACRO BUILD_TABLES( SOURCE, OUTPUT );

%BUILD_PROTOCOL ( &SOURCE, &OUTPUT );
%BUILD_PAYMENTS ( &SOURCE, &OUTPUT );
%BUILD_INDICATI ( &SOURCE, &OUTPUT );
%BUILD_PROCEDUR ( &SOURCE, &OUTPUT );
%BUILD_STUDYLVL ( &SOURCE, &OUTPUT );
%BUILD_CLAB ( &SOURCE, &OUTPUT );
%BUILD_INVESTIG ( &SOURCE, &OUTPUT );
%BUILD_QASTATUS( &SOURCE, &OUTPUT );

%MEND BUILD_TABLES;

**********************************************************;
**********************************************************;
******               END OF MACRO                   ******;
**********************************************************;
**********************************************************;

%XLSX2SAS( &HYGFOLDER, datafile, &DATAFILE ); 
/*SHEETNAME=Sheet1 );*/

data pull_list;
	set datafile;
	where company ne ' ' or protocol ne ' ';
run;


data _NULL_	;
	set pull_list;
	tname = cats("PEX" ,_N_);
	call execute( '%PDC2SAS( '||tname||', '||Company||', '||Protocol||' )' );
run;

data cmbtlb;
	set PEX:;
run;

%BUILD_TABLES( cmbtlb, &HYGFOLDER );


********************************************************************;
*	Build Receipt
********************************************************************;

proc sql;
	create table protocol_cnt as
	select company, code, count(*) as PROTOCOL
	from protocol_cnvt2
	group by code
	order by code;

	create table investig_cnt as
	select distinct company, code, count(*) as INVESTIG
	from investig_cnvt2
	group by code
	order by code;

	create table payments_cnt as
	select distinct company, code, count(*) as PAYMENTS
	from payments_cnvt2
	group by code
	order by code;
quit;

data recv_sum;	
	merge protocol_cnt investig_cnt payments_cnt;
	by code;
run;

%SAS2XLSX( recv_sum, PDC_PULL_Count, RECV_CNT );




