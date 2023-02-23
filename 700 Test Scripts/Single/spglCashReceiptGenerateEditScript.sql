DO
$$
DECLARE l_cashreceiptitems jsonb;
    l_manualitems jsonb;
    l_gltransactionid int;
    l_output varchar;
BEGIN

l_gltransactionid := (select max(id) from gltransactionsubtypecashreceipt);

SELECT jsonb_agg(json_build_object('glentryid', pInvG.id, 'amount', r.amount*-1, 'glreconciliationid', r.id))
INTO l_cashreceiptitems
FROM glEntry pPTI
JOIN glReconciliation r
     ON pPTI.ID = r.glEntryIDFrom
JOIN GLEntry pInvG
     ON r.glEntryIDTo = pInvG.ID
WHERE pPTI.glentrytypeid = 4 -- payment toward invoice
    AND pPTI.gltransactionid = l_gltransactionid;

SELECT json_agg(json_build_object('amount', pPTI.Amount*-1,'description', pPTI.description,'glaccountid', pPTI.glaccountid,'glcostcentreid', pPTI.glcostcentreid,'billingaccountid',pPTI.Rowidchargedto))
INTO l_manualitems
FROM glEntry pPTI
WHERE pPTI.glentrytypeid not in (1,4)
    AND pPTI.gltransactionid = l_gltransactionid;

SELECT
'call spglCashReceipt (
p_barcode :=null,
p_crmcontactidpaidfor:='||coalesce(cr.crmcontactidpaidfor,0)::varchar||',
p_crmcontactiduser:='||coalesce(cr.crmcontactidpaidfor,0)::varchar||',
p_glbatchid:='||coalesce(tx.glbatchid,0)::varchar||',
p_glpaymentmethodid:='||coalesce(cr.glpaymentmethodid,0)::varchar||',
p_gltransactionid:='||coalesce(tx.id,0)::varchar||',
p_name:='||coalesce(''''||cr.name||'''','null')::varchar||',
p_receiptamount:='||cr.amount::varchar||',
p_referencenumber:='||coalesce(''''||tx.referencenumber||'''','null')::varchar||',
p_transactiondate:='''||tx.transactiondate::varchar||'''::date,
p_transactionstate:=''edit'',
p_writeoffamount:=null,
p_cashreceiptitems:='||coalesce(''''||l_cashreceiptitems::varchar||'''','null')||',
p_manualitems:='||coalesce(''''||l_manualitems::varchar||'''','null')||',
p_syschangehistoryid:=2);'
into l_output
from glTransactionSubTypeCashReceipt cr
join gltransaction tx on tx.id=cr.id
WHERE cr.ID = l_gltransactionid;

raise notice '%', l_output;

END
$$
