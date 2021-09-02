SELECT cases.accountid, cases.id, rt.name, createddate
FROM salesforce.cases
JOIN salesforce.recordtype rt
ON cases.recordtypeid = rt.id