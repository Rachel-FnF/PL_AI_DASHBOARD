"""
SQL 쿼리 모음 파일
- 모든 분석에서 사용하는 SQL 쿼리를 여기에 모아둡니다
- 새로운 분석을 추가할 때는 여기에 SQL 쿼리를 추가하세요

사용 방법:
    from config.sql_queries import get_brand_domestic_query
    
    sql = get_brand_domestic_query(yyyymm='202509', yyyymm_py='202409', brd_cd='M')
"""


def get_brand_domestic_query(yyyymm, yyyymm_py, brd_cd):
    """
    브랜드별 내수 손익분석 쿼리 (01번 분석)
    
    Args:
        yyyymm: 당해 년월 (예: '202509')
        yyyymm_py: 전년 동월 (예: '202409')
        brd_cd: 브랜드 코드 (예: 'M')
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT PST_YYYYMM,
           CORP_CD,
           CORP_NM,
           BRD_CD,
           BRD_NM,
           CHNL_TYPE,
           SUM(TAG_SALE_AMT) as TAG_SALE_AMT,                    -- 택가
           SUM(ACT_SALE_AMT) as ACT_SALE_AMT,                    -- 실판가+
           SUM(VAT_EXC_ACT_SALE_AMT) as VAT_EXC_ACT_SALE_AMT,   -- 실판가-
           SUM(COGS) as COGS,                                    -- 매출원가
           SUM(SALE_TTL_PRFT) - SUM(DSTRB_CMS) AS 매출총이익,    -- 매출총이익
           SUM(DCST) as DCST,                                    -- 직접비TTL
           SUM(SALE_STFF_CMS) as SALE_STFF_CMS,                  -- 판매직수수료
           SUM(SHOP_RNT) as SHOP_RNT,                           -- 매장임차료
           SUM(LGT_CST) as LGT_CST,                            -- 매장물류비
           SUM(IDCST) as IDCST,                                 -- 간접비TTL
           SUM(AD_CST) as AD_CST,                               -- 광고선전비
           SUM(LAB_CST) as LAB_CST,                             -- 인건비
           SUM(COMM_LAB_CST) as COMM_LAB_CST,                   -- 공통인건비
           SUM(COMM_ALLOC_CST) as COMM_ALLOC_CST,               -- 공통비배부
           SUM(SALE_TTL_PRFT) - SUM(DSTRB_CMS) - SUM(DCST) - SUM(IDCST) AS 영업이익
    FROM SAP_FNF.DM_PL_CHNL_M
    WHERE PST_YYYYMM IN ('{yyyymm_py}', '{yyyymm}') 
      AND CHNL_TYPE = '내수'
      AND BRD_CD = '{brd_cd}'
    GROUP BY PST_YYYYMM, CORP_CD, CORP_NM, CHNL_TYPE, BRD_CD, BRD_NM
    """


def get_brand_export_query(yyyymm, yyyymm_py, brd_cd):
    """
    브랜드별 수출 손익분석 쿼리 (02번 분석)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT PST_YYYYMM,
           CORP_CD,
           CORP_NM,
           BRD_CD,
           BRD_NM,
           CHNL_TYPE,
           SUM(TAG_SALE_AMT) as TAG_SALE_AMT,
           SUM(ACT_SALE_AMT) as ACT_SALE_AMT,
           SUM(VAT_EXC_ACT_SALE_AMT) as VAT_EXC_ACT_SALE_AMT,
           SUM(COGS) as COGS,
           SUM(SALE_TTL_PRFT) - SUM(DSTRB_CMS) AS 매출총이익,
           SUM(DCST) as DCST,
           SUM(SALE_STFF_CMS) as SALE_STFF_CMS,
           SUM(SHOP_RNT) as SHOP_RNT,
           SUM(LGT_CST) as LGT_CST,
           SUM(IDCST) as IDCST,
           SUM(AD_CST) as AD_CST,
           SUM(LAB_CST) as LAB_CST,
           SUM(COMM_LAB_CST) as COMM_LAB_CST,
           SUM(COMM_ALLOC_CST) as COMM_ALLOC_CST,
           SUM(SALE_TTL_PRFT) - SUM(DSTRB_CMS) - SUM(DCST) - SUM(IDCST) AS 영업이익
    FROM SAP_FNF.DM_PL_CHNL_M
    WHERE PST_YYYYMM IN ('{yyyymm_py}', '{yyyymm}') 
      AND CHNL_TYPE = '수출'
      AND BRD_CD = '{brd_cd}'
    GROUP BY PST_YYYYMM, CORP_CD, CORP_NM, CHNL_TYPE, BRD_CD, BRD_NM
    """


def get_channel_profit_loss_query(yyyymm, yyyymm_py, brd_cd):
    """
    채널별 손익분석 쿼리 (03번 분석)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    WITH t1 AS (
        SELECT a.PST_YYYYMM,
               CASE
                   WHEN TO_NUMBER(LEFT(a.PST_YYYYMM, 4)) = YEAR(CURRENT_DATE) THEN '당해'::text
                   WHEN TO_NUMBER(LEFT(a.PST_YYYYMM, 4)) = YEAR(DATEADD(YEAR, -1, CURRENT_DATE)) THEN '전년'::text
                   ELSE '기타'::text
               END AS PYCY,
               a.BRD_CD,
               a.BRD_NM,
               a.CHNL_CD,
               a.CHNL_NM,
               a.SHOP_CD,
               a.SHOP_NM,
               SUM(TAG_SALE_AMT) as TAG_SALE_AMT,
               SUM(ACT_SALE_AMT) as ACT_SALE_AMT,
               SUM(RVSL_BEF_COGS) as RVSL_BEF_COGS,
               SUM(VLTN_RVSL_AMT) as VLTN_RVSL_AMT,
               SUM(RVSL_AFT_COGS) as RVSL_AFT_COGS,
               SUM(DSTRB_CMS) as DSTRB_CMS,
               SUM(ACT_SALE_AMT) / 1.1 - SUM(RVSL_AFT_COGS) + SUM(VLTN_AMT) - SUM(DSTRB_CMS) as GROSS_PRFT,
               SUM(RYT) as RYT,
               SUM(SHOP_RNT) as SHOP_RNT,
               CASE
                   WHEN a.CHNL_CD = '1' THEN SUM(SM_CMS)
                   WHEN a.CHNL_CD = '2' THEN SUM(DF_SALE_STFF_CMS)
                   WHEN a.CHNL_CD IN ('3', '7','11','12') THEN SUM(DMGMT_SALE_STFF_CMS)
                   WHEN a.CHNL_CD IN ('4', '5') THEN SUM(ALNC_ONLN_CMS)
                   ELSE 0
               END as CMS,
               SUM(SM_CMS) as SM_CMS,
               SUM(DF_SALE_STFF_CMS) as DF_SALE_STFF_CMS,
               SUM(DMGMT_SALE_STFF_CMS) as DMGMT_SALE_STFF_CMS,
               SUM(ALNC_ONLN_CMS) as ALNC_ONLN_CMS,
               SUM(SHOP_DEPRC_CST) as SHOP_DEPRC_CST,
               SUM(CARD_CMS) as CARD_CMS,
               SUM(LGT_CST) + SUM(STRG_CST) as LGT_STRG_CST
        FROM SAP_FNF.dm_prft_shop_m a
        JOIN SAP_FNF.dm_dcst_shop_m b
            ON a.PST_YYYYMM = b.PST_YYYYMM
            AND a.CORP_CD = b.CORP_CD
            AND a.BRD_CD = b.BRD_CD
            AND a.CHNL_CD = b.CHNL_CD
            AND a.SHOP_CD = b.SHOP_CD
        WHERE TO_NUMBER(a.PST_YYYYMM) BETWEEN
            TO_NUMBER(TO_CHAR(DATEADD(YEAR, -1, DATE_TRUNC('YEAR', CURRENT_DATE)), 'YYYYMM'))
            AND TO_NUMBER(TO_CHAR(DATEADD(MONTH, -1, CURRENT_DATE), 'YYYYMM'))
          AND a.CHNL_CD NOT IN ('9')
          AND a.BRD_CD = '{brd_cd}'
        GROUP BY a.PST_YYYYMM, a.BRD_CD, a.BRD_NM, a.CHNL_CD, a.CHNL_NM, a.SHOP_CD, a.SHOP_NM
    )
    SELECT pst_yyyymm,
           RIGHT(PST_YYYYMM,2)||'월'::text as month,
           PYCY,
           brd_cd,
           chnl_cd,
           chnl_nm,
           SUM(tag_sale_amt) as tag_sale_amt,
           SUM(ACT_SALE_AMT) as ACT_SALE_AMT,
           SUM(RVSL_BEF_COGS) as RVSL_BEF_COGS,
           SUM(VLTN_RVSL_AMT) as VLTN_RVSL_AMT,
           SUM(RVSL_AFT_COGS) as RVSL_AFT_COGS,
           SUM(DSTRB_CMS) as DSTRB_CMS,
           SUM(GROSS_PRFT) as GROSS_PRFT,
           SUM(RYT) as RYT,
           SUM(SHOP_RNT) as SHOP_RNT,
           SUM(CMS) as CMS,
           SUM(SHOP_DEPRC_CST) as SHOP_DEPRC_CST,
           SUM(CARD_CMS) as CARD_CMS,
           SUM(LGT_STRG_CST) as LGT_STRG_CST,
           SUM(RYT) + SUM(SHOP_RNT) + SUM(CMS) + SUM(SHOP_DEPRC_CST) + SUM(CARD_CMS) + SUM(LGT_STRG_CST) as DCST,
           SUM(GROSS_PRFT) - SUM(RYT) - SUM(SHOP_RNT) - SUM(CMS) - SUM(SHOP_DEPRC_CST) - SUM(CARD_CMS) - SUM(LGT_STRG_CST) as DPRFT
    FROM t1
    WHERE PST_YYYYMM IN ('{yyyymm_py}','{yyyymm}')
    GROUP BY pst_yyyymm, RIGHT(PST_YYYYMM,2), pycy, brd_cd, chnl_cd, chnl_nm
    """


def get_product_sales_query(yyyymm, brd_cd):
    """
    제품별 매출분석 쿼리 (04번 분석)
    
    Args:
        yyyymm: 당해 년월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT PST_YYYYMM,
           BRD_CD,
           BRD_NM,
           LARGE_CLASS_NM,
           MIDDLE_CLASS_NM,
           ITEM_NM,
           SESN,
           SUM(SALE_QTY) as SALE_QTY,
           SUM(TAG_SALE_AMT) as TAG_SALE_AMT,
           SUM(ACT_SALE_AMT) as ACT_SALE_AMT,
           SUM(VAT_EXC_ACT_SALE_AMT) as VAT_EXC_ACT_SALE_AMT,
           SUM(MILE_SALE_AMT) as MILE_SALE_AMT,
           SUM(RVSL_BEF_COGS) as RVSL_BEF_COGS,
           SUM(RVSL_AFT_COGS) as RVSL_AFT_COGS,
           SUM(COGS) as COGS,
           SUM(SALE_TTL_PRFT) as SALE_TTL_PRFT
    FROM sap_fnf.DM_PRFT_SHOP_PRDT_GL_M
    WHERE PST_YYYYMM = '{yyyymm}' 
      AND BRD_CD = '{brd_cd}'
    GROUP BY PST_YYYYMM, BRD_CD, BRD_NM, LARGE_CLASS_NM, MIDDLE_CLASS_NM, ITEM_NM, SESN
    ORDER BY SALE_TTL_PRFT DESC
    LIMIT 100
    """


def get_ad_expense_total_query(yyyymm, yyyymm_py, brd_cd):
    """
    광고선전비 전체 합계 쿼리 (07번 분석 - 합계용)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT pst_yyyymm,
           SUM(ttl_use_amt) as total_amt
    FROM sap_fnf.dm_idcst_cctr_m
    WHERE pst_yyyymm IN ('{yyyymm_py}', '{yyyymm}')
      AND ctgr1 = '광고선전비'
      AND brd_cd = '{brd_cd}'
    GROUP BY pst_yyyymm
    ORDER BY pst_yyyymm
    """


def get_ad_expense_detail_query(yyyymm, yyyymm_py, brd_cd):
    """
    광고선전비 세부 내역 쿼리 (07번 분석 - 세부용)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT pst_yyyymm,
           brd_cd,
           brd_nm,
           ctgr1,
           ctgr2,
           ctgr3,
           gl_nm,
           SUM(ttl_use_amt) as ttl_use_amt
    FROM sap_fnf.dm_idcst_cctr_m
    WHERE pst_yyyymm IN ('{yyyymm_py}', '{yyyymm}')
      AND ctgr1 = '광고선전비'
      AND brd_cd = '{brd_cd}'
    GROUP BY pst_yyyymm, brd_cd, brd_nm, ctgr1, ctgr2, ctgr3, gl_nm
    ORDER BY ttl_use_amt DESC
    """


def get_ad_expense_trend_query(trend_months, brd_cd):
    """
    광고선전비 12개월 추세 쿼리 (07번 분석 - 추세용)
    
    Args:
        trend_months: 12개월 월 리스트 (예: ['202410', '202411', ...])
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    trend_months_str = "', '".join(trend_months)
    return f"""
    SELECT pst_yyyymm,
           SUM(ttl_use_amt) as total_amt
    FROM sap_fnf.dm_idcst_cctr_m
    WHERE pst_yyyymm IN ('{trend_months_str}')
      AND ctgr1 = '광고선전비'
      AND brd_cd = '{brd_cd}'
    GROUP BY pst_yyyymm
    ORDER BY pst_yyyymm
    """


def get_indirect_cost_query(yyyymm, yyyymm_py, brd_cd):
    """
    간접비 분석 쿼리 (10번 분석)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT pst_yyyymm,
           CASE
               WHEN TO_NUMBER(LEFT(pst_yyyymm, 4)) = YEAR(CURRENT_DATE) THEN '당해'::text
               WHEN TO_NUMBER(LEFT(pst_yyyymm, 4)) = YEAR(DATEADD(YEAR, -1, CURRENT_DATE)) THEN '전년'::text
           END AS year_type,
           brd_cd,
           brd_nm,
           ctgr1,
           ctgr2,
           ctgr3,
           GL_CD,
           GL_NM,
           SUM(ttl_use_amt) as total_amount
    FROM sap_fnf.dm_idcst_cctr_m
    WHERE pst_yyyymm IN ('{yyyymm_py}', '{yyyymm}')
      AND brd_cd = '{brd_cd}'
    GROUP BY pst_yyyymm, brd_cd, brd_nm, ctgr1, ctgr2, ctgr3, GL_CD, GL_NM
    ORDER BY ctgr1, ctgr2, ctgr3, GL_CD, GL_NM, pst_yyyymm
    """


def get_direct_cost_query(yyyymm, yyyymm_py, brd_cd):
    """
    직접비 분석 쿼리 (11번 분석)
    
    Args:
        yyyymm: 당해 년월
        yyyymm_py: 전년 동월
        brd_cd: 브랜드 코드
    
    Returns:
        str: SQL 쿼리
    """
    return f"""
    SELECT a.pst_yyyymm,
           CASE
               WHEN TO_NUMBER(LEFT(a.pst_yyyymm, 4)) = YEAR(CURRENT_DATE) THEN '당해'::text
               WHEN TO_NUMBER(LEFT(a.pst_yyyymm, 4)) = YEAR(DATEADD(YEAR, -1, CURRENT_DATE)) THEN '전년'::text
           END AS year_type,
           a.brd_cd,
           a.brd_nm,
           a.chnl_cd,
           a.chnl_nm,
           SUM(a.act_sale_amt) as total_sales,
           SUM(b.ryt) as ryt,
           SUM(b.shop_rnt) as shop_rnt,
           SUM(b.sm_cms) as sm_cms,
           SUM(b.df_sale_stff_cms) as df_sale_stff_cms,
           SUM(b.dmgmt_sale_stff_cms) as dmgmt_sale_stff_cms,
           SUM(b.alnc_onln_cms) as alnc_onln_cms,
           SUM(b.shop_deprc_cst) as shop_deprc_cst,
           SUM(b.card_cms) as card_cms,
           SUM(b.lgt_cst) as lgt_cst,
           SUM(b.strg_cst) as strg_cst,
           SUM(b.ryt + b.shop_rnt + b.sm_cms + b.df_sale_stff_cms + b.dmgmt_sale_stff_cms + 
               b.alnc_onln_cms + b.shop_deprc_cst + b.card_cms + b.lgt_cst + b.strg_cst) as total_direct_cost
    FROM sap_fnf.dm_prft_shop_m a
    JOIN sap_fnf.dm_dcst_shop_m b
        ON a.pst_yyyymm = b.pst_yyyymm
        AND a.corp_cd = b.corp_cd
        AND a.brd_cd = b.brd_cd
        AND a.chnl_cd = b.chnl_cd
        AND a.shop_cd = b.shop_cd
    WHERE a.pst_yyyymm IN ('{yyyymm_py}', '{yyyymm}')
      AND a.brd_cd = '{brd_cd}'
      AND a.chnl_cd NOT IN ('9')
    GROUP BY a.pst_yyyymm, a.brd_cd, a.brd_nm, a.chnl_cd, a.chnl_nm
    ORDER BY a.chnl_cd, a.pst_yyyymm
    """


def get_channel_sales_trend_query(yyyymm_start, yyyymm_end, brd_cd):
    """
    채널별 매출 분석 쿼리 (12개월 추이 - 기간, 채널, 아이템)
    
    Args:
        yyyymm_start: 시작 년월 (예: '202409')
        yyyymm_end: 종료 년월 (예: '202509')
        brd_cd: 브랜드 코드 (예: 'M')
    
    Returns:
        str: SQL 쿼리
    
    설명:
        - 12개월 추이 데이터를 조회
        - 채널별 매출 분석
        - 클래스3(아이템)별 매출 분석
        - 채널 내 순위, 전체 순위 포함
    """
    return f"""
    WITH raw AS (
        SELECT pst_yyyymm,
               CASE 
                   WHEN b.mgmt_chnl_cd = '4' THEN '자사몰'
                   WHEN b.mgmt_chnl_cd = '5' THEN '제휴몰'
                   WHEN b.mgmt_chnl_cd IN ('3', '11', 'C3') THEN '직영점'
                   WHEN b.mgmt_chnl_nm LIKE '아울렛%' THEN '아울렛'
                   ELSE b.mgmt_chnl_nm
               END AS chnl_nm,
               c.prdt_hrrc3_nm AS class3,
               SUM(a.act_sale_amt) AS sale_amt
        FROM sap_fnf.dm_pl_shop_prdt_m a
        JOIN sap_fnf.mst_shop b 
            ON a.brd_cd = b.brd_cd
           AND a.shop_cd = b.sap_shop_cd
        JOIN sap_fnf.mst_prdt c
            ON a.prdt_cd = c.prdt_cd
        WHERE 1=1
          AND a.corp_cd = '1000'
          AND a.brd_cd = '{brd_cd}'
          AND a.chnl_cd NOT IN ('0', '8', '9', '99')
          AND a.pst_yyyymm BETWEEN '{yyyymm_start}' AND '{yyyymm_end}'
        GROUP BY 1, 2, 3
    ), main AS (
        SELECT pst_yyyymm,
               chnl_nm,
               class3,
               sale_amt,
               sale_amt_chnl_ttl,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm ORDER BY sale_amt_chnl_ttl DESC) AS in_yymm_rnk,
               DENSE_RANK() OVER(PARTITION BY pst_yyyymm, chnl_nm ORDER BY sale_amt DESC) AS in_chnl_rnk
        FROM (
            SELECT pst_yyyymm,
                   chnl_nm,
                   class3,
                   sale_amt,
                   SUM(sale_amt) OVER(PARTITION BY pst_yyyymm, chnl_nm) AS sale_amt_chnl_ttl
            FROM raw
        )
    )
    SELECT pst_yyyymm,
           chnl_nm,
           class3,
           sale_amt,
           sale_amt_chnl_ttl,
           CASE WHEN sale_amt_chnl_ttl = 0 THEN 0 ELSE ROUND(sale_amt / sale_amt_chnl_ttl * 100) END AS sale_ratio,
           in_yymm_rnk,
           in_chnl_rnk
    FROM main 
    ORDER BY pst_yyyymm DESC, in_yymm_rnk, in_chnl_rnk
    """


# ============================================================================
# 새로운 SQL 쿼리를 추가할 때는 위와 같은 형식으로 함수를 만들어주세요
# ============================================================================
# 예시:
# def get_my_new_query(yyyymm, brd_cd):
#     return f"""
#     SELECT ...
#     FROM ...
#     WHERE ...
#     """

