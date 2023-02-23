/*

 BK This routine is only partially converted

 spCashReceipt needs to be updated with the following code:

 Comments
 /*
https://developer.moneris.com/en/More/Testing/Testing%20a%20Solution
https://developer.moneris.com/More/Testing/Penny%20Value%20Simulator

General transaction test numbers:
MasterCard (MC) 5454545454545454
(MC) (bin2 CAN) 2222400041240011
Visa 4242424242424242
Amex 373599005095005
JCB 3566007770015365
Diners 36462462742008
Discover 6011000992927602
Track 2 5258968987035454=06061015454001060101?
UnionPay 6250944000000771 Expiry date: 12/49 (MM/l_MICurr), CVN2 value '371'

 Parameters
  p_Environment varchar=NULL -- One of {global, prod, uat, dev}.
, p_SubmitToGateway BOOLEAN DEFAULT TRUE -- If the payment should be submitted to the gateway.
, p_AuthorizationNo varchar DEFAULT NULL -- AuthorizationNo if the user supplied an existing one that existed from some other call to the payment gateway.
, p_AuthHeader varchar DEFAULT NULL -- Authorization token.
, p_CreditCardNumber BIGINT DEFAULT NULL -- Credit card number if we need to submit to the gateway.
, p_CreditCardExpiryYear INT DEFAULT NULL, p_CreditCardExpiryMonth INT DEFAULT NULL
, p_CreditCardCVV varchar DEFAULT NULL
, p_CardholderName varchar DEFAULT NULL -- Name on credit card (needed because it may differ from name we have for crmContact in the database).

  Variables

    l_tblRequest                        MONERISREQUESTTABLETYPE;
    l_tblResponse                       MONERISRESPONSETABLETYPE;
    l_requestClientNumber  INT;
    l_statusID             varchar;
    l_statusText           varchar;
    l_responseStatus       varchar;
    l_responseMessage      varchar  := '';
    l_expiryDate           varchar;
    l_oInfo                varchar := '';
    l_successText          varchar   := 'Approved';
    l_isCreditCard         BOOLEAN;
    l_expiryMonthString    varchar;

 Source Code

 When Moneris is implemented we will store orderid in referencenumber
 for credit cards rather than referencenumber.  There is a number of
 inserts/update statements this affects

       CASE WHEN l_isCreditCard = TRUE THEN p_orderID
            ELSE p_referenceNumber
            END

The following code is at the end of spCashReceipt

*******************************************************************************************************

    IF l_isCreditCard = TRUE AND p_SubmitToGateway = TRUE
    THEN
        l_expiryMonthString := p_creditCardExpiryMonth::varchar;
        IF LENGTH(l_expiryMonthString) = 1
        THEN
            l_expiryMonthString := '0' || l_expiryMonthString;
        END IF;

-- this needs Format YYYY-MM-DD
        l_expiryDate := p_creditCardExpiryYear::varchar || '-' || l_expiryMonthString || '-01';

        IF p_crmContactIDPaidFor IS NOT NULL
        THEN
            l_requestClientNumber := (
                                         SELECT ContactNumber
                                         FROM crmContact
                                         WHERE ID = p_crmContactIDPaidFor);
        END IF;

        l_requestClientNumber := COALESCE(l_requestClientNumber, 0);

        INSERT INTO l_tblRequest(
            ID, OrderReference, ClientReference, CreditCardNumber, SecurityCode, ExpiryDate, CardholderName, Amount)
        VALUES (
                   l_cashReceiptOrderID,
                   l_cashReceiptOrderID,
                   l_requestClientNumber,
                   p_creditCardNumber,
                   p_creditCardCVV,
                   l_expiryDate,
                   p_CardholderName,
                   p_ReceiptAmount);

        IF p_debugLevel >= 1
        THEN
            SELECT * FROM l_tblRequest;

            IF p_debugLevel >= 2
            THEN
GOTO ResumeDebugLevelTwo;
            END IF;
        END IF;

-- call the gateway.
        INSERT INTO l_tblResponse
            CALL spMoneris(
            p_Environment=
            l_environment, p_CallType=
            'Purchase', p_AuthHeader=
            l_authHeader, p_Data=
            l_tblRequest, l_oMessage=
            l_oMessage
            OUTPUT, l_oInfo=
            l_oInfo
            OUTPUT);

        IF p_debugLevel >= 1
        THEN
            SELECT l_oInfo INFO;
            SELECT * FROM l_tblResponse;
        END IF;

-- a purchase request should have returned us one row.
        IF (
               SELECT COUNT(0)
               FROM l_tblResponse) != 1
        THEN
            -- Unexpected error: row count returned from payment procedure did not equal 1
            RAISE SQLSTATE '51049';
        END IF;

        SELECT COALESCE(Status, 'unknown'),
               COALESCE(MESSAGE, 'unknown'),
               TransactionTag,
               AuthorizationNumber
        INTO l_responseStatus, l_responseMessage , l_transactionTag, p_authorizationNo
        FROM l_tblResponse
        LIMIT 1;

        IF p_debugLevel >= 1
        THEN
            SELECT l_responseStatus  responseStatus,
                   l_responseMessage responseMessage,
                   l_transactionTag  transactionTag,
                   p_authorizationNo authorizationNo;
        END IF;

        -- What denotes 'Approved'? If you  to this url:
-- https://developer.moneris.com/More/Testing/Penny%20Value%20Simulator
-- There are many examples starting with 'Approved...', so that is what we will  with here.
        IF LEFT(COALESCE(l_responseStatus, ''), LENGTH(l_successText)) != l_successText
        THEN
            --Payment not completed
            RAISE SQLSTATE '51050' USING MESSAGE = l_responseStatus || '-' || l_responseMessage;
        END IF;

        UPDATE glTransactionSubTypeCashReceipt
        SET TransactionTag=l_transactionTag, AuthorizationNo=p_authorizationNo
        WHERE ID = p_glTransactionID;

        --ResumeDebugLevelTwo:
    END IF;

*/

/*
*******************************************************************************************************

The following code is a new function that needs to be created.  Once again it is partially converted

DROP FUNCTION IF EXISTS S0000V0000.spMoneris CASCADE;
CREATE OR REPLACE FUNCTION S0000V0000.spMoneris(p_Environment varchar = NULL, -- One of {global, prod, uat, dev}.
                                            p_CallType varchar, -- One of {Purchase, Refund, Refunds}.
                                            p_AuthHeader varchar, -- Authorization token.
                                            p_Data MONERISREQUESTTABLETYPE -- Requests to submit. Can have multiple rows.
)
    RETURNS BOOLEAN
AS
$$
DECLARE
    DECLARE
    l_Items         TABLE
                    (
                        DATACOLUMN XML
                    );
    DECLARE
    l_tblResponse   MONERISRESPONSETABLETYPE;
    l_ErrorMessage  varchar;
    l_ErrorSeverity INT;
    l_ErrorState    INT;
    l_ErrorNumber   INT;
    l_token         INT;
    l_ret           INT;
    l_oMessage      varchar;
    l_oInfo         varchar;
-- These next declarations/defaults are finicky.
-- START OF DO NOT CHANGE TYPE, SIZE, OR DEFAULT VALUE --
    l_authHeader    varchar;
    l_contentType   varchar;
    l_postData      varchar;
    l_url           varchar;
    l_statusID      varchar   = '-1'; -- Start this off as -1 to see if we even get the first result back.
    l_statusText    varchar  = '';
    l_responseText  varchar = '';
    l_json          varchar;
-- END OF DO NOT CHANGE TYPE, SIZE, OR DEFAULT VALUE --
    l_connected     BIT           = 0;
    l_settingName   varchar  = 'paymentGateway';
    l_setting1      varchar  = 'url';

BEGIN

    SET l_contentType = 'application/json';
    IF COALESCE(p_Environment, '') NOT IN ('global', 'prod', 'uat', 'dev')
    THEN
        p_Environment := 'global';
    END IF;
    SELECT setting2
    INTO l_url
    FROM Setting
    WHERE SettingName = l_settingName
      AND Setting1 = l_setting1
      AND Environment = p_Environment
    LIMIT 1;
    IF (l_url IS NULL)
        AND (p_Environment != 'global')
    THEN
        SELECT setting2
        INTO l_url
        FROM Setting
        WHERE SettingName = l_settingName
          AND Setting1 = l_setting1
          AND Environment = 'global'
        LIMIT 1;
    END IF;
    IF l_url IS NULL
    THEN
        l_statusID := '1';
        l_statusText := 'Unable to determine the url for the ' || p_Environment || ' environment.';
GOTO done;
    END IF;
    IF RIGHT(l_url, 1) != '/'
    THEN
        l_url := l_url || '/';
    END IF;
    l_url := l_url || p_CallType;

--SET l_url = 'https://services.cpaastaging.ca/Moneris/' || p_CallType

    l_authHeader := 'Bearer ' || p_AuthHeader;
    IF p_CallType = 'Purchase'
    THEN
        l_postData :=
                (
                    SELECT ID,
                           OrderReference,
                           COALESCE(ClientReference, '') AS ClientReference,
                           CreditCardNumber,
                           SecurityCode,
                           ExpiryDate,
                           CardholderName,
                           Amount
                    FROM p_Data-- FOR JSON AUTO
                );
    ELSE
        l_postData :=
                (
                    SELECT ID,
                           OrderReference,
                           COALESCE(ClientReference, '') AS ClientReference,
                           TransactionNumber,
                           Amount
                    FROM p_Data FOR-- JSON AUTO
                );
    END IF;
    l_postData := COALESCE(l_postData, '');
    IF l_postData = ''
    THEN
        l_statusID := '2';
        l_statusText := 'No data to send.';
GOTO done;
    END IF;
    IF p_CallType = 'Purchase'
        OR p_CallType = 'Refund'
    THEN
-- The listener at the 'purchase' and 'refund' endpoints does not like the outer surrounding square brackets.
        IF LEFT(l_postData, 1) = ''
        THEN
            l_postData := SUBSTRING(l_postData, 2, LEN(l_postData) - 1);
        END IF;
        IF RIGHT(l_postData, 1) = ''
        THEN
            l_postData := SUBSTRING(l_postData, 1, LEN(l_postData) - 1);
        END IF;
    END IF;

-- Open the connection.

-- Calls to SP_OACREATE, sp_OAMethod, sp_OAGetProperty, sp_OADestroy
-- are all MS SQL specific.  Postgres doesn't supports COM objects, because it is
-- multiplatform project, and COM objects are +/- MS platform only technology.
-- This will need to be changed for Postgres with new calls.

    EXEC
    l_ret = SP_OACREATE
                'MSXML2.ServerXMLHTTP'
            ,
            l_token OUT;
    IF l_ret != 0
    THEN
        l_statusID := '3';
        l_statusText := 'Unable to connect to the server.';
GOTO done;
    END IF;
    l_connected := 1;
    EXEC
    l_ret = sp_OAMethod
                l_token
            ,
            'open'
            ,
            NULL
            ,
            'POST'
            ,
            l_url
            ,
            'false';
    IF l_ret != 0
    THEN
        l_statusID := '4';
        l_statusText := 'Unable to open the connection.';
GOTO closeConnection;
    END IF;
    EXEC
    l_ret = sp_OAMethod
                l_token
            ,
            'setRequestHeader'
            ,
            NULL
            ,
            'Authorization'
            ,
            l_authHeader;
    EXEC
    l_ret = sp_OAMethod
                l_token
            ,
            'setRequestHeader'
            ,
            NULL
            ,
            'Content-type'
            ,
            l_contentType;

-- Send the request.
    EXEC
    l_ret = sp_OAMethod
                l_token
            ,
            'send'
            ,
            NULL
            ,
            l_postData;

-- Handle the response.
    EXEC
    l_ret = sp_OAGetProperty
                l_token
            ,
            'status'
            ,
            l_statusID OUT;
    EXEC
    l_ret = sp_OAGetProperty
                l_token
            ,
            'statusText'
            ,
            l_statusText OUT;
    EXEC
    l_ret = sp_OAGetProperty
                l_token
            ,
            'responseText'
            ,
            l_responseText OUT;
    closeConnection:
    EXEC
    l_ret = sp_OADestroy
        l_token;
    IF l_ret = 0
    THEN
        l_connected := 0;
    END IF;

    l_statusID := COALESCE(l_statusID, 'unknown');
    l_statusText := COALESCE(l_statusText, 'unknown');
    l_responseText := COALESCE(l_responseText, '');
    l_postData := COALESCE(l_postData, '');
    l_url := COALESCE(l_url, '');
    IF l_statusID != '200'
    THEN
-- A status other than 200 is usually more of a technical issue.
        l_oMessage := p_CallType || ' not completed, status text = ''' || l_statusText || ''', status ID = ' ||
                      l_statusID;
GOTO Done;
    END IF;

    -- Preserve l_responseText so we can return it as information in its raw form.
-- We will work with l_json instead.
    l_json := l_responseText;
    IF LEFT(l_json, 1) = ''
    THEN
        l_json := SUBSTRING(l_json, 2, LEN(l_json) - 1);
    END IF;
    IF RIGHT(l_json, 1) = ''
    THEN
        l_json := SUBSTRING(l_json, 1, LEN(l_json) - 1);
    END IF;
    l_json := '<Items>' || REPLACE(
            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(l_json, '},{', '}{'), '":', '='), '{"', '<row '), '}', ' />'), ',"',
                    ' '), '=null', '=""') || '</Items>';
    INSERT INTO l_Items
    SELECT l_json;
    INSERT INTO l_tblResponse
    SELECT T.c.value('l_ID', 'varchar')                   ID,
           T.c.value('l_OrderReference', 'varchar')       OrderReference,
           T.c.value('l_TransactionTag', 'varchar')      TransactionTag,
           T.c.value('l_AuthorizationNumber', 'varchar') AuthorizationNumber,
           T.c.value('l_Status', 'varchar')              Status,
           T.c.value('l_Message', 'varchar')             Message
    FROM l_Items
    JOIN LATERAL ( datacolumn.nodes('/Items/row') AS T(C)) T
    ON TRUE;
    UPDATE l_tblResponse
    SET Message = CASE
        WHEN Message = 'The payment was successfully.'
            THEN 'The payment was successful.'
        WHEN Message = 'The refund was successfully.'
            THEN 'The refund was successful.'
        ELSE Message
        END;
    IF CATCH
    THEN
        L_ERRORMESSAGE := ERROR_MESSAGE();
        l_ErrorSeverity := ERROR_SEVERITY();
        l_ErrorState := ERROR_STATE();
        l_ErrorNumber := ERROR_NUMBER();
        IF L_CONNECTED = 1
        THEN
            TRY
                EXEC
            l_ret = sp_OADestroy
                l_token;
            IF L_RET = 0
            THEN
                SET L_CONNECTED = 0;
            END IF;
        END TRY THEN CATCH
    END CATCH;

    L_OMESSAGE := p_CallType || ' not completed, an exception was raised: ErrorNumber=' ||
                         COALESCE(l_ErrorNumber::varchar, '(unknown)') ||
                         COALESCE(', ' || LEFT(l_ErrorMessage, 200), '');
       END CATCH
       done:
        L_OINFO := LEFT('statusID=' || l_statusID || ', statusText=' || l_statusText || ', l_postData=' || l_postData ||
                   ', responseText=' || l_responseText || ', convertedText=' || l_json || ', url=' || COALESCE(l_url, ''),
                   4000);
        SELECT ID,
OrderReference,
TransactionTag,
AuthorizationNumber,
Status,
MESSAGE
FROM l_tblResponse;
        END;

$$ LANGUAGE plpgsql;

*/

/*
*******************************************************************************************************

       In MS SQL we defined custom types which we referenced below.  Here is the code that defines the
       types that will need to be converted to Postgres once we activate the Moneris portion of
       cash receipting.

        IF NOT EXISTS (SELECT 1 FROM dbo.systypes WHERE name = 'MonerisRequestTableType' AND xtype = 243)
        BEGIN
            CREATE	TYPE dbo.MonerisRequestTableType AS TABLE (
                ID			INT,
                OrderReference		varchar,
                ClientReference		varchar,
                CreditCardNumber	varchar,
                SecurityCode		varchar,
                ExpiryDate		varchar,
                CardholderName		varchar,
                TransactionNumber	varchar,
                Amount			DECIMAL(19, 2)
                )
        END
        GO

        IF NOT EXISTS (SELECT 1 FROM dbo.systypes WHERE name = 'MonerisResponseTableType' AND xtype = 243)
        BEGIN
            CREATE	TYPE dbo.MonerisResponseTableType AS TABLE (
                ID			varchar,
                OrderReference		varchar,
                TransactionTag		varchar,
                AuthorizationNumber	varchar,
                [Status]		varchar,
                [Message]		varchar
                )
        END

 */