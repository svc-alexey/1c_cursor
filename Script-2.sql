WITH TenderData AS (
    SELECT 
        c.checkid,
        MAX(tm.objectnumber) AS tendernum
    FROM transdb.check_detail c
    INNER JOIN transdb.checks cc ON c.checkid = cc.checkid
    INNER JOIN transdb.tender_media_detail t ON c.checkdetailid = t.checkdetailid
    INNER JOIN transdb.tender_media tm ON tm.tendmedid = t.tendmedid
    INNER JOIN transdb.revenue_center rc ON rc.revctrid = c.revctrid
    WHERE
        cc.checkclose BETWEEN TO_TIMESTAMP('2025-09-21 00:00:00','YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP('2025-09-21 23:59:59','YYYY-MM-DD HH24:MI:SS')
        AND rc.objectnumber in (32)
        AND cc.checkid in (4432250,4434247,4434248,4434249,4434250,4434251,4434254,4434293,4434300,4434372,4434401,4434450,4434460,4434466,4434485,4434493,4434517,4434602,4434682,4434757,4434802,4434806,4434836,4434845,4434872,4434893,4434956,4435062,4435063,4435080,4435147,4435175,4435528,4435565,4435626,4435630,4435637,4435687,4435693,4435760,4435775,4435801,4435824,4435828,4435831,4435859,4435876,4435881,4435966,4435967,4436030,4436038,4436045,4436046,4436050,4436059,4436105,4436197,4436205,4436235,4436277,4436388,4436426,4436516,4436529,4436619,4436866,4436897,4437005,4437029,4437034,4437096,4437119,4437122,4437129,4437131,4437136,4437152,4437169,4437181,4437230,4437239,4437268,4437314,4437324,4437331,4437337,4437343,4437375,4437391,4437419,4437421,4437440,4437443,4437497,4437502,4437509,4437521,4437523,4437525,4437526,4437527,4437549,4437620,4437625,4437628,4437633,4437636,4437637,4437642,4437645,4437649)
        AND c.detailtype = 4
        AND SUBSTR(cc.STATUS, 23, 1) = '0'
        AND SUBSTR(cc.STATUS, 18, 1) = '0'
        AND cc.reopenedtochecknum IS NULL
        AND cc.ADDEDTOCHECKNUM IS NULL
        AND SUBSTR(c.status, 10, 1) = '0'
    GROUP BY c.checkid
),
DiscountData AS (
    SELECT 
        c.checkid,
        c.detailindex,
        SUM(c.total) AS total,
        MAX(s1.stringtext) AS discountname
    FROM transdb.check_detail c
    INNER JOIN transdb.discount_detail d ON c.checkdetailid = d.checkdetailid
    INNER JOIN transdb.discount dd ON dd.dscntid = d.dscntid
    INNER JOIN transdb.string_table s1 ON s1.stringnumberid = dd.nameid AND s1.langid = 1
    INNER JOIN transdb.checks cc ON c.checkid = cc.checkid
    INNER JOIN transdb.revenue_center rc ON rc.revctrid = c.revctrid
    WHERE
        cc.checkclose BETWEEN TO_TIMESTAMP('2025-09-21 00:00:00','YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP('2025-09-21 23:59:59','YYYY-MM-DD HH24:MI:SS')
        AND rc.objectnumber in (32)
        AND cc.checkid in (4432250,4434247,4434248,4434249,4434250,4434251,4434254,4434293,4434300,4434372,4434401,4434450,4434460,4434466,4434485,4434493,4434517,4434602,4434682,4434757,4434802,4434806,4434836,4434845,4434872,4434893,4434956,4435062,4435063,4435080,4435147,4435175,4435528,4435565,4435626,4435630,4435637,4435687,4435693,4435760,4435775,4435801,4435824,4435828,4435831,4435859,4435876,4435881,4435966,4435967,4436030,4436038,4436045,4436046,4436050,4436059,4436105,4436197,4436205,4436235,4436277,4436388,4436426,4436516,4436529,4436619,4436866,4436897,4437005,4437029,4437034,4437096,4437119,4437122,4437129,4437131,4437136,4437152,4437169,4437181,4437230,4437239,4437268,4437314,4437324,4437331,4437337,4437343,4437375,4437391,4437419,4437421,4437440,4437443,4437497,4437502,4437509,4437521,4437523,4437525,4437526,4437527,4437549,4437620,4437625,4437628,4437633,4437636,4437637,4437642,4437645,4437649)
        AND c.detailtype = 2
        AND SUBSTR(cc.STATUS, 23, 1) = '0'
        AND SUBSTR(cc.STATUS, 18, 1) = '0'
        AND cc.reopenedtochecknum IS NULL
        AND cc.ADDEDTOCHECKNUM IS NULL
        AND SUBSTR(c.status, 10, 1) = '0'
    GROUP BY c.checkid, c.detailindex
),
AggregatedDiscountData AS (
    SELECT
        td.checkid,
        td.tendernum,
        dd.total AS misum,
        dd.discountname,
        dd.detailindex
    FROM TenderData td
    LEFT JOIN DiscountData dd ON td.checkid = dd.checkid
),
PreChecksDetail AS (
    SELECT
        d.checkid,
		 d.detailindex,
        TO_CHAR(d.OBJECTNUMBER) AS stringobjectnum,
        rc.objectnumber AS rc_objectnumber,
        s5.stringtext AS hu_stringtext,
        cc.checknumber,
        TO_CHAR(cc.checkopen, 'DD-MM-YYYY HH24:MI:SS') AS checkopenday,
        TO_CHAR(cc.checkclose, 'DD-MM-YYYY HH24:MI:SS') AS checkcloseday,
        mg.objectnumber AS mg_objectnumber,
        s3.stringtext AS md_stringtext,
        ROUND(d.numerator / d.denominator, 3) AS quantity,
        d.total,
        ROUND(d.total / NULLIF(d.numerator / d.denominator, 0), 3) AS price,
        e.checkname,
        ws.objectnumber AS ws_objectnumber,
        stax.STRINGTEXT AS tax,
        tax.TAXCLASSID AS idtax
    FROM transdb.menu_item_detail m
    LEFT JOIN transdb.check_detail d ON d.checkdetailid = m.checkdetailid
    LEFT JOIN transdb.checks cc ON cc.checkid = d.checkid
    LEFT JOIN transdb.menu_item_definition md ON m.menuitemdefid = md.menuitemdefid
    LEFT JOIN transdb.menu_item_price mp ON m.menuitempriceid = mp.menuitempriceid
    LEFT JOIN transdb.menu_item_master mm ON md.menuitemmasterid = mm.menuitemmasterid
    LEFT JOIN transdb.major_group mg ON (mg.objectnumber = mm.majgrpobjnum AND mg.hierstrucid = 1)
    LEFT JOIN transdb.family_group fg ON (fg.objectnumber = mm.famgrpobjnum AND fg.hierstrucid = 1)
    LEFT JOIN transdb.hierarchy_unit hu ON hu.revctrid = d.revctrid
    LEFT JOIN transdb.hierarchy_structure hs ON hs.hierunitid = hu.hierunitid
    LEFT JOIN transdb.employee e ON e.employeeid = cc.employeeid
    LEFT JOIN transdb.cashier csh ON cc.cashierid = csh.cashierid
    LEFT JOIN transdb.secure_detail sd ON sd.checkdetailid = d.checkdetailid + 1
    LEFT JOIN transdb.revenue_center rc ON rc.revctrid = d.revctrid
    LEFT JOIN transdb.dining_table t ON t.diningtableid = cc.diningtableid AND t.hierstrucid = hs.hierstrucid
    LEFT JOIN transdb.string_table s1 ON s1.stringnumberid = mg.nameid AND s1.langid = 1
    LEFT JOIN transdb.string_table s2 ON s2.stringnumberid = fg.nameid AND s2.langid = 1
    LEFT JOIN transdb.string_table s3 ON s3.stringnumberid = md.name1id AND s3.langid = 1
    LEFT JOIN transdb.string_table s5 ON s5.stringnumberid = hu.nameid AND s5.langid = 1
    LEFT JOIN transdb.string_table s6 ON s6.stringnumberid = csh.nameid AND s6.langid = 1
    LEFT JOIN transdb.string_table s7 ON s7.stringnumberid = t.nameid AND s7.langid = 1
    LEFT JOIN (
        SELECT 
            ttls.checkid, 
            MAX(ttls.transactiontime), 
            ttls.workstationid, 
            ttls.ordtypeid
        FROM transdb.totals ttls
        INNER JOIN (
            SELECT 
                checkid, 
                MAX(transactiontime) maxtime 
            FROM transdb.totals 
            GROUP BY checkid
        ) ttls1 ON ttls.checkid = ttls1.checkid AND ttls.transactiontime = ttls1.maxtime
        GROUP BY ttls.checkid, ttls.workstationid, ttls.ordtypeid
    ) ttl ON cc.checkid = ttl.checkid
    LEFT JOIN transdb.workstation ws ON ttl.workstationid = ws.workstationid
    LEFT JOIN transdb.MENU_ITEM_CLASS mic ON md.MENUITEMCLASSOBJNUM = mic.OBJECTNUMBER AND mic.hierstrucid = 2
    LEFT JOIN transdb.TAX_CLASS tax ON mic.taxclassobjnum = tax.objectnumber AND tax.hierstrucid = 2
    LEFT JOIN transdb.STRING_TABLE stax ON tax.NAMEID = stax.STRINGNUMBERID AND stax.langid = 1
    WHERE
        cc.checkclose BETWEEN TO_TIMESTAMP('2025-09-21 00:00:00','YYYY-MM-DD HH24:MI:SS') AND TO_TIMESTAMP('2025-09-21 23:59:59','YYYY-MM-DD HH24:MI:SS')
        AND rc.objectnumber in (32)
        AND cc.checkid in (4432250,4434247,4434248,4434249,4434250,4434251,4434254,4434293,4434300,4434372,4434401,4434450,4434460,4434466,4434485,4434493,4434517,4434602,4434682,4434757,4434802,4434806,4434836,4434845,4434872,4434893,4434956,4435062,4435063,4435080,4435147,4435175,4435528,4435565,4435626,4435630,4435637,4435687,4435693,4435760,4435775,4435801,4435824,4435828,4435831,4435859,4435876,4435881,4435966,4435967,4436030,4436038,4436045,4436046,4436050,4436059,4436105,4436197,4436205,4436235,4436277,4436388,4436426,4436516,4436529,4436619,4436866,4436897,4437005,4437029,4437034,4437096,4437119,4437122,4437129,4437131,4437136,4437152,4437169,4437181,4437230,4437239,4437268,4437314,4437324,4437331,4437337,4437343,4437375,4437391,4437419,4437421,4437440,4437443,4437497,4437502,4437509,4437521,4437523,4437525,4437526,4437527,4437549,4437620,4437625,4437628,4437633,4437636,4437637,4437642,4437645,4437649)
        AND SUBSTR(cc.STATUS, 23, 1) = '0'
        AND SUBSTR(cc.STATUS, 18, 1) = '0'
        AND d.detailtype = 1
        AND cc.reopenedtochecknum IS NULL
        AND cc.ADDEDTOCHECKNUM IS NULL
        AND hs.hierid = 1
        AND s2.stringtext != 'TXT MESSAGE'
),
RankedPreChecksDetail AS (
    SELECT
        pcd.*,
        SUM(pcd.quantity) OVER (PARTITION BY pcd.checkid, pcd.stringobjectnum) as net_quantity,
        MAX(SIGN(pcd.quantity)) OVER (PARTITION BY pcd.checkid) as check_type,
        CASE WHEN pcd.quantity > 0 THEN ROW_NUMBER() OVER (PARTITION BY pcd.checkid, pcd.stringobjectnum ORDER BY pcd.detailindex ASC) ELSE NULL END as sale_rank,
        MAX(CASE WHEN TRUNC(pcd.quantity) != pcd.quantity THEN 1 ELSE 0 END) OVER (PARTITION BY pcd.checkid, pcd.stringobjectnum) as has_fractional
    FROM PreChecksDetail pcd
),
FilteredPreChecksDetail AS (
    SELECT *
    FROM RankedPreChecksDetail
    WHERE
        check_type = -1
        OR
        (
            check_type = 1 AND net_quantity > 0 AND (
                (has_fractional = 1 AND quantity > 0)
                OR
                (has_fractional = 0 AND sale_rank <= net_quantity)
            )
        )
),
ChecksDetail AS (
    SELECT
        checkid,
        stringobjectnum,
        detailindex,
        rc_objectnumber AS objectnumber,
        hu_stringtext AS stringtext,
        checknumber,
        checkopenday,
        checkcloseday,
        mg_objectnumber AS objectnumber_mg,
        md_stringtext AS stringtext_md,
        quantity AS total_quantity,
        price,
        total AS total_sum,
        SUM(total) OVER (PARTITION BY checkid) AS total_sales_per_check,
        checkname,
        ws_objectnumber AS objectnumber_ws,
        tax AS TAX,
        idtax AS IDTAX
    FROM FilteredPreChecksDetail
),
ChecksWithDiscountContext AS (
    SELECT
        cd.*,
        agg.misum,
        agg.discountname,
        agg.tendernum,
        agg.detailindex AS discount_detailindex,
        SUM(CASE WHEN cd.detailindex < agg.detailindex THEN cd.total_sum ELSE 0 END) OVER (PARTITION BY cd.checkid) AS total_for_discount_base
    FROM ChecksDetail cd
    LEFT JOIN AggregatedDiscountData agg ON cd.checkid = agg.checkid
)
SELECT
    cwdc.objectnumber AS НомерТочкиПродаж,
    cwdc.stringtext AS НаименованиеТочкиПродаж,
    cwdc.objectnumber_ws AS НомерКассы,
    cwdc.checknumber AS НомерЧека,
    cwdc.checkid AS ИДЧека,
    cwdc.checkopenday AS ДатаОткрытияЧекаСтрока,
    cwdc.checkcloseday AS ДатаЗакрытияЧекаСтрока,
    cwdc.stringobjectnum AS КодТовара,
    cwdc.stringtext_md AS НазваниеТовара,
    cwdc.total_quantity AS КоличествоТовара,
    cwdc.price AS ЦенаТовара,
    cwdc.total_sum AS СуммаТовара,
    cwdc.total_sales_per_check AS СтоимостьЧекаБезСкидки,
    NVL(-cwdc.misum, 0) AS СуммаСкидкиЧека,
    cwdc.total_sales_per_check - NVL(-cwdc.misum, 0) AS СтоимостьЧека,
    CAST(
        CASE
            WHEN cwdc.discount_detailindex IS NOT NULL AND cwdc.detailindex >= cwdc.discount_detailindex THEN 0
            WHEN NVL(cwdc.total_for_discount_base, 0) = 0 THEN 0
            ELSE NVL(cwdc.total_sum, 0) / cwdc.total_for_discount_base * NVL(-cwdc.misum, 0)
        END AS NUMBER(15, 3)
    ) AS СуммаСкидкиСтроки,
    NVL(cwdc.IDTAX, 0) AS КодСтавкиНДС,
    cwdc.TAX AS СтавкаНДССтрокой,
    cwdc.discountname AS НаименованиеСкидкиЧека,
    cwdc.tendernum AS НомерМетодаОплаты,
    cwdc.checkname AS ИмяСотрудника,
	cwdc.detailindex AS НомерПозицииВЧеке,
	cwdc.discount_detailindex AS НомерСтрокиСкидки
FROM ChecksWithDiscountContext cwdc
ORDER BY cwdc.checknumber, cwdc.detailindex