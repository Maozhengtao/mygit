#!/usr/bin/python
#-*-coding : UTF-8 -*-
skc_run_sql = {
	'sql_skc' : '''
SELECT
	sk.ID AS skc,

IF (
	sp.cate3 IN (
		198,
		200,
		196,
		105,
		155,
		125,
		106,
		156,
		126,
		130,
		132,
		166,
		211,
		159,
		154,
		164,
		199,
		197,
		157,
		170
	),
	sp.cate3,
	sp.Cate2
) AS cate,
 p.Price AS price,
 avg(IF(bd.`Status` IN(2, 3), 1, 0)) AS tj,
 count(DISTINCT bd.ID) AS sent_cnt
FROM
	boxdetail AS bd
INNER JOIN box AS b ON b.ID = bd.BoxID
INNER JOIN product AS p ON p.ID = bd.ProdID
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spu AS sp ON sp.ID = P.SPUID
WHERE
	bd.UPC <> 0
AND bd.Deleted = 0
AND b.`Status` = 5
AND sp.cate2 NOT IN (34, 43, 49, 45)
AND bd.userID NOT IN (
	SELECT
		au.userID
	FROM
		authroleuser AS au
)
GROUP BY
	sk.ID
HAVING
	price > 20'''
	,
'sql_boxlog' : '''
SELECT
	bl.SerialID,
	min(bl.UpdatedDate) AS UpdatedDate
FROM
	boxlog AS bl
WHERE
	bl.Field = 'Status'
AND bl.NewValue = 2
AND bl.oldvalue = 1
GROUP BY
	bl.SerialID
	'''
	,
	'sql_sid_size' :
	'''
	SELECT
	sk.ID AS skc,
	ps.Size,
	count(DISTINCT pu.upc) AS cnt
FROM
	productupc AS pu
INNER JOIN product AS p ON p.ID = pu.ProdID
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
INNER JOIN spupresize AS sps ON sps.ID = p.PreSizeID
INNER JOIN productsize AS ps ON ps.ID = sps.Size
WHERE
	pu.`Status` = 1
AND pu.Deleted = 0
AND pu.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
AND sp.Cate1 IN (1, 2, 3)
AND sps.Size NOT IN (
	SELECT
		spi.SizeID
	FROM
		specialsize AS spi
		where deleted = 0
)
AND sk.ID IN (% s)
GROUP BY
	sk.ID,
	ps.Size'''
	,
	'sql_stock' : '''
	SELECT
	sk.ID,
	count(DISTINCT pu.upc) AS cnt
FROM
	productupc AS pu
INNER JOIN product AS p ON p.ID = pu.ProdID
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spupresize AS sps ON sps.ID = p.PreSizeID
WHERE
	pu.`Status` = 1
AND pu.Deleted = 0
AND pu.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 3 MONTH)
AND sps.Size NOT IN (
	SELECT
		spi.SizeID
	FROM
		specialsize AS spi
		where deleted = 0
)
AND sk.ID IN (%s)
GROUP BY
	sk.ID''',
	'sql_boxcnt' : '''
	SELECT
	round(avg(r.cnt)) AS boxcnt
FROM
	(
		SELECT
			date(b.CreatedDate) AS date,
			count(DISTINCT b.ID) AS cnt
		FROM
			box AS b
		WHERE
			b.Deleted = 0
		AND date(b.CreatedDate) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
		AND WEEKDAY(b.CreatedDate) NOT IN (5, 6)
		GROUP BY
			date
	) AS r'''
	,
	'sql_skc_item' : '''
	SELECT
	r.cate,
	pc. NAME AS 品类名,
	r.skc,
	r.品牌,
	r.负责人,
	r.商品名,
	r.价格,
	r.推荐率,
	r.在库库存,
	r.空中库存,
	r.近三十天发出
FROM
	(
		SELECT
			sp.Cate1 AS cate,

		IF (
			sp.cate3 IN (
				198,
				200,
				196,
				105,
				155,
				125,
				106,
				156,
				126,
				130,
				132,
				166,
				211,
				159,
				154,
				164,
				199,
				197,
				157,
				170
			),
			sp.cate3,
			sp.Cate2
		) AS cate_1,
		sk.ID AS skc,
		pb. NAME AS 品牌,
		pb.ChargePerson AS 负责人,
		p.`Name` AS '商品名',
		p.price AS '价格',
		t.tj AS '推荐率',
		ifnull(c.stock, 0) AS '在库库存',
		ifnull(a.`空中库存`, 0) AS '空中库存',
		IFNULL(d.cnt, 0) AS '近三十天发出'
	FROM
		productupc AS pu
	INNER JOIN product AS p ON p.ID = pu.ProdID
	INNER JOIN skc AS sk ON sk.Id = p.SKCID
	INNER JOIN spu AS sp ON sp.ID = p.SPUID
	INNER JOIN productbrand AS pb ON pb.ID = sp.Brand
	#INNER JOIN USER AS u ON u.ID = pb.ChargePerson
	LEFT JOIN (
		SELECT
			sk.ID,
			count(DISTINCT pu.upc) AS stock
		FROM
			productupc AS pu
		INNER JOIN product AS p ON p.ID = pu.ProdID
		INNER JOIN skc AS sk ON sk.ID = p.skcID
		WHERE
			pu.Deleted = 0
		AND pu.`Status` = 1
		GROUP BY
			sk.ID
	) AS c ON c.id = sk.id
	LEFT JOIN (
		SELECT
			sk.ID AS skc,
			count(DISTINCT bd.ID) AS 空中库存
		FROM
			boxdetail AS bd
		INNER JOIN product AS p ON p.ID = bd.ProdID
		INNER JOIN box AS b ON b.ID = bd.BoxID
		INNER JOIN productupc AS pu ON pu.ProdID = p.ID
		INNER JOIN skc AS sk ON sk.ID = p.SKCID
		WHERE
			b. STATUS = 7
		AND bd.Deleted = 0
		AND bd.upc <> 0
		AND bd. STATUS = 6
		GROUP BY
			sk.ID
	) AS a ON a.skc = sk.ID
	LEFT JOIN (
		SELECT
			sk.ID AS skc,
			count(DISTINCT bd.ID) AS cnt
		FROM
			boxdetail AS bd
		INNER JOIN product AS p ON p.ID = bd.ProdID
		INNER JOIN skc AS sk ON sk.ID = p.SKCID
		WHERE
			bd.Deleted = 0
		AND bd.UPC <> 0
		AND bd.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
		GROUP BY
			skc
	) AS d ON d.skc = sk.ID
	LEFT JOIN (
		SELECT
			p.SKCID,
			avg(IF(bd.`Status` IN(2, 3), 1, 0)) AS tj
		FROM
			boxdetail AS bd
		INNER JOIN box AS b ON b.ID = bd.boxID
		INNER JOIN product AS p ON p.ID = bd.ProdID
		WHERE
			bd.Deleted = 0
		AND bd.UPC <> 0
		AND b. STATUS = 5
		AND b.deleted = 0
		GROUP BY
			p.SKCID
	) AS t ON t.SKCID = sk.ID
	WHERE
		p.Deleted = 0
	AND pb.Deleted = 0
	AND sk.Deleted = 0
	AND sp.Deleted = 0
	AND pu.Deleted = 0
	AND sk.ID IN (% s) #and sk.id = 	10280
	GROUP BY
		sk.ID
	) AS r
INNER JOIN productcate AS pc ON pc.ID = r.cate_1'''
	,
	'boxdetail_sql' :
	'''
	SELECT

IF (
	sp.cate3 IN (
		198,
		200,
		196,
		105,
		155,
		125,
		106,
		156,
		126,
		130,
		132,
		166,
		211,
		159,
		154,
		164,
		199,
		197,
		157,
		170
	),
	sp.cate3,
	sp.Cate2
) AS cate_1,
 bd.Price AS price,
 sk.ID AS skc,
 bd.BoxID,
 bd.CreatedDate AS datetime
FROM
	boxdetail AS bd
INNER JOIN product AS p ON p.ID = bd.ProdID
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
WHERE
	bd.Deleted = 0
AND bd.UPC <> 0
AND bd.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 2 MONTH)
''',
'skcchange_sql' : '''
SELECT
	spi.SKCID AS skc,
	spi.Common AS 'type',
	spi.CreatedDate AS datetime
FROM
	specialinventory AS spi
WHERE
	spi.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
GROUP BY
	skc,
	datetime'''
	,
'sql_newskc' : '''
SELECT
	r.ID AS skc
FROM
	(
		SELECT
			sk.ID,
			min(pi.CreatedDate) AS min
		FROM
			productinventory AS pi
		INNER JOIN product AS p ON p.ID = pi.ProdID
		INNER JOIN skc AS sk ON sk.ID = p.SKCID
		WHERE
			pi.Type = 0
		AND pi.Deleted = 0
		GROUP BY
			sk.ID
		HAVING
			min >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
	) AS r'''
,
'sql_newskc_item' : '''
SELECT
	sk.ID AS skc,
	pb. NAME AS '品牌',
	pb.ChargePerson AS '负责人',
	p. NAME AS '商品名',
	DATE(min(pi.CreatedDate)) AS '最近入库时间',
	count(pi.UPC) AS '入库数量',
	ifnull(r2.cnt, 0) AS '7天内发出次数'
FROM
	productinventory AS pi
INNER JOIN product AS p ON p.ID = pi.ProdID
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
INNER JOIN productbrand AS pb ON pb.ID = sp.Brand
LEFT JOIN (
	SELECT
		sk.ID,
		count(DISTINCT bd.ID) AS cnt
	FROM
		productinventory AS pi
	INNER JOIN product AS p ON p.ID = pi.ProdID
	INNER JOIN skc AS sk ON sk.ID = p.SKCID
	INNER JOIN (
		SELECT
			sk.ID,
			min(pi.CreatedDate) AS min
		FROM
			productinventory AS pi
		INNER JOIN product AS p ON p.ID = pi.ProdID
		INNER JOIN skc AS sk ON sk.ID = p.SKCID
		WHERE
			pi.Type = 0
		AND pi.Deleted = 0
		GROUP BY
			sk.ID
		HAVING
			min >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
	) AS r1 ON r1.ID = sk.ID
	LEFT JOIN boxdetail AS bd ON bd.UPC = pi.UPC
	WHERE
		pi.Type = 0
	AND pi.CreatedDate >= r1.min
	AND bd.UPC <> 0
	AND bd.Deleted = 0
	AND bd.CreatedDate >= pi.CreatedDate
	AND bd.CreatedDate <= DATE_ADD(
		pi.CreatedDate,
		INTERVAL 7 DAY
	)
	AND bd.UserID NOT IN (
		SELECT
			au.userID
		FROM
			authroleuser AS au
	)
	GROUP BY
		sk.ID
) AS r2 ON r2.ID = sk.ID
WHERE
	pi.Type = 0
AND pi.Deleted = 0
GROUP BY
	sk.ID
HAVING
	最近入库时间 >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
'''
,
'sql_skc_stop' : '''
SELECT
	odp.SKCID
FROM
	`order` AS od
INNER JOIN orderproduct AS odp ON odp.OrderID = od.ID
LEFT JOIN productinventory AS pi ON pi.SKCID = odp.SKCID
AND pi.CreatedDate > odp.CreatedDate
WHERE
	od.Type = 1
AND od.Deleted = 0
AND odp.Deleted = 1
AND pi.SKCID IS NULL
UNION
	SELECT
		sk.ID AS skc
	FROM
		skc AS sk
	LEFT JOIN productinventory AS pi ON pi.SKCID = sk.ID
	AND pi.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 1 MONTH)
	WHERE
		sk.Note LIKE "%停产%"
	AND pi.skcID IS NULL
'''
,
'sql_size_trans' : 
'''
SELECT
	p.ID as sku,
	sk.ID AS skc,
	ps.size AS size_a,
	sps.Size AS size_a_ID,
	cs.size AS size_b,
	sps.Size1 AS size_b_ID,
	ifnull(r.sale_rate,0) as sale_rate
FROM
	product AS p
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spupresize AS sps ON sps.ID = p.PreSizeID
INNER JOIN productsize AS ps ON sps.Size = ps.ID
INNER JOIN collarsize AS cs ON cs.ID = sps.Size1
LEFT JOIN (
	SELECT
		p.ID,
		sum(IF(pu.`Status` = 3, 1, 0)) / count(DISTINCT pi.upc) AS sale_rate
	FROM
		productinventory AS pi
	INNER JOIN product AS p ON p.ID = pi.ProdID
	INNER JOIN productupc AS pu ON pu.UPC = pi.UPC
	WHERE
		pi.Type IN (0, 2)
	AND pi.Deleted = 0
	AND p.SKCID = %s
	GROUP BY
		p.ID
) AS r ON r.ID = p.ID
WHERE
	sk.ID = %s
AND p.Deleted = 0
AND sps.Deleted = 0
AND sps.Size NOT IN (
	SELECT
		spi.SizeID
	FROM
		specialsize AS spi 
	where  deleted = 0 
)
''',

'sql_aim_skc' : 
'''
SELECT
	0 AS IsNew,
	sp.ID AS SPUID,
	sk.ID AS SKCID,
	p.ID AS SKUID,
	sp.SerialNumber,
	sp.Brand AS Brand,
	p.PurchPrice,
	"" AS PurchaseNum,
	date_format(DATE_ADD(CURDATE(), INTERVAL 30 DAY),'%%Y-%%m-%%d') AS DeliveryDate,
	sp.cate1 AS Cate1
FROM
	product AS p
INNER JOIN skc AS sk ON sk.ID = p.SKCID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
INNER JOIN spupresize AS sps ON sps.SPUID = p.SPUID
WHERE
	sk.id = %s
GROUP BY
	sk.id
''',
'sql_order_cnt':
'''
SELECT
	od.ID,
	odp.SKUID,
	sum(odp.PurchaseNum) AS order_cnt
FROM
	`order` AS od
INNER JOIN orderproduct AS odp ON odp.OrderID = od.ID
WHERE
	od.`Status` = 5
AND od.Deleted = 0
AND odp.Deleted = 0
AND odp.DeliveryDate < DATE_ADD(CURDATE(), INTERVAL 1 MONTH)
GROUP BY
	odp.SKUID
''',
'RecommendSizeBottom_1' :
'''
SELECT
  u.RecommendSizeBottom AS 'cate2'
FROM
  USER AS u
WHERE
  u.Deposit = 2
AND u.Deleted = 0
AND u.RecommendSizeBottom != '0'
AND u.RecommendSizeBottom != "该用户相近用户的尺码太平均，请联系错过 记录该用户信息" 
''',
'RecommendSizeTop_1' : 
  '''
SELECT
  replace(u.RecommendSizeTop,' ','') AS 'cate1'
FROM
  USER AS u
WHERE
  u.Deposit = 2
AND u.Deleted = 0
AND RecommendSizeTop not like "%% %%"
AND RecommendSizeTop != "0"
''',
'RecommendSizeShoes_1':
'''
SELECT
	3 AS cate,
	ps.Size AS size1,
	count(DISTINCT bd.ID) AS rate
FROM
	boxdetail AS bd
INNER JOIN product AS p ON p.ID = bd.ProdID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
INNER JOIN spupresize AS sps ON sps.ID = p.PreSizeID
INNER JOIN productsize AS ps ON ps.ID = sps.Size
WHERE
	sp.cate1 = 3
AND sps.Size NOT IN (
	SELECT
		SizeID
	FROM
		specialsize
	where deleted = 0
)
AND bd.`Status` IN (2, 3)
and bd.Deleted = 0 
and bd.UPC <> 0 
AND bd.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY
	ps.Size
''',
'RecommendSizeSuit_1':
'''
SELECT
	5 AS cate,
	ps.Size AS size1,
	count(DISTINCT bd.ID) AS rate
FROM
	boxdetail AS bd
INNER JOIN product AS p ON p.ID = bd.ProdID
INNER JOIN spu AS sp ON sp.ID = p.SPUID
INNER JOIN spupresize AS sps ON sps.ID = p.PreSizeID
INNER JOIN productsize AS ps ON ps.ID = sps.Size
WHERE
	sp.cate1 = 5
AND sps.Size NOT IN (
	SELECT
		SizeID
	FROM
		specialsize 
	where deleted = 0
)
AND bd.`Status` IN (2, 3)
and bd.Deleted = 0 
and bd.UPC <> 0 
AND bd.CreatedDate >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
GROUP BY
	ps.Size
''',
'sql_s_skc_record' : '''
SELECT
	count(DISTINCT r.skc) AS skc_cnt,
	sum(r.S_skc) AS s_stock_cnt,
	sum(IF(r.S_skc = 1, r.stock, 0)) / sum(r.S_skc) AS s_avg_stock,
	date(
		DATE_SUB(CURDATE(), INTERVAL 1 DAY)
	) AS date
FROM
	(
		SELECT
			p.SKCID AS skc,
			count(DISTINCT pu.upc) AS stock,

		IF (p.SKCID IN(% s), 1, 0) AS 'S_skc'
		FROM
			productupc AS pu
		INNER JOIN product AS p ON p.ID = pu.ProdID
		INNER JOIN (
			SELECT
				P.SKCID
			FROM
				boxdetail AS bd
			INNER JOIN product AS p ON p.ID = bd.ProdID
			WHERE
				bd.Deleted = 0
			AND bd.`Status` IN (2, 3)
			AND bd.UPC <> 0
			GROUP BY
				p.SKCID
		) AS r ON r.SKCID = p.SKCID
		WHERE
			pu.`Status` = 1
		AND pu.Deleted = 0
		GROUP BY
			p.SKCID
	) AS r
'''
}
f = open('./data/skc_run_sql.txt', 'w')
f.write(str(skc_run_sql))
f.close()