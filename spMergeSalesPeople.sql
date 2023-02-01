/*
  Purpose: This stored procedure is to merge two sales people's records, 
           including their sales orders, history, etc.
  Database: AdventureWorks2019
           https://learn.microsoft.com/en-us/sql/samples/adventureworks-install-configure?view=sql-server-ver16&tabs=ssms
  Affecting tables: 9 tables
  Transaction: Use transaction, commit and rollback. 
  Error handling: When error occurs, it will rollback (no update to the tables) and raise an error.
                  "Msg 50000, Level 16, State 0, Procedure dbo.spMergeSalesPerson, Line ..."
  Run Script: at the bottom 
*/

create or alter procedure dbo.spMergeSalesPeople (
    @EmployeeID_MergeFrom int,
	@EmployeeID_MergeTo int,
	@AddressLine1 nvarchar(60), 
	@AddressLine2 nvarchar(60),
	@City nvarchar(30), 
	@StateProvinceID int,  
	@PostalCode nvarchar(15), 
	@SpatialLocation nvarchar(60), 
	@AddressTypeID_MergeTo int ) as
begin	

 begin try
  begin transaction T1 

     --- Table 1: SalesOrderHeader: switch BusinessEntityID value
	 update Sales.SalesOrderHeader set SalesPersonID=@EmployeeID_MergeTo where SalesPersonID=@EmployeeID_MergeFrom

    --- Table 2 & 3: BusinessEntityAddress & Address
    -- delete MergeFrom's records in BusinessEntityAddress & Address   
    select AddressID
	into #AddressIDs_MergeFrom
	from Person.BusinessEntityAddress  where BusinessEntityID=@EmployeeID_MergeFrom  
	
    delete from Person.BusinessEntityAddress  
	       where BusinessEntityID=@EmployeeID_MergeFrom  
    
	delete from Person.Address 
	       where AddressID in (select AddressID
				               from #AddressIDs_MergeFrom)

    drop table #AddressIDs_MergeFrom

   -- add MergeTo's records in BusinessEntityAddress & Address if not existed 
   if not exists (select AddressID      
	              from Person.BusinessEntityAddress 
				  where BusinessEntityID=@EmployeeID_MergeTo and
				        AddressTypeID=@AddressTypeID_MergeTo) 
      begin		  
	       -- When TO business entity does not have Address record:	     		   
		  declare @NewAddressID int
		  set @NewAddressID=0

		  insert into Person.Address (AddressLine1,AddressLine2,City,StateProvinceID,PostalCode,SpatialLocation,rowguid,ModifiedDate)
		  values (@AddressLine1,  @AddressLine2, @City, @StateProvinceID, @PostalCode, @SpatialLocation, newid(), getdate())
		  
		  set @NewAddressID=scope_identity() 
            
		  insert into Person.BusinessEntityAddress(BusinessEntityID,AddressID,AddressTypeID,rowguid,ModifiedDate)
		  values (@EmployeeID_MergeTo, @NewAddressID, @AddressTypeID_MergeTo, newid(), getdate())
		  
	  end

	----------- Table 4: SalesTerritoryHistory: switch BusinessEntityID value
	update Sales.SalesTerritoryHistory
	set BusinessEntityID=@EmployeeID_MergeTo,
		rowguid=newid(),
		MOdifiedDate=getdate()
	where BusinessEntityID=@EmployeeID_MergeFrom 

	----------- Table 5: SalesPersonQuotaHistory: switch BusinessEntityID value	
	update Sales.SalesPersonQuotaHistory
	set BusinessEntityID=@EmployeeID_MergeTo,
		rowguid=newid(),
		ModifiedDate=getdate()
	where BusinessEntityID=@EmployeeID_MergeFrom and
		  not exists (select *
					  from Sales.SalesPersonQuotaHistory t
					  where t.QuotaDate=QuotaDate and
							t.BusinessEntityID=BusinessEntityID)

	delete from Sales.SalesPersonQuotaHistory where BusinessEntityID=@EmployeeID_MergeFrom 

   --- Table 6: Store: switch SalesPersonID value  
   update Sales.Store
   set SalesPersonID=@EmployeeID_MergeTo, 
       rowguid=newid(), ModifiedDate=getdate()
   where SalesPersonID=@EmployeeID_MergeFrom 


   --- Table 7 : SalesPerson: add some $ amounts over  
   declare @SalesQuota money
   declare @Bonus money
   declare @SalesYTD money
   declare @SalesLastYear money

   select @SalesQuota=@SalesQuota,
          @Bonus=Bonus,
          @SalesYTD=SalesYTD,
          @SalesLastYear=SalesLastYear
   from Sales.SalesPerson 
   where BusinessEntityID=@EmployeeID_MergeFrom

   update Sales.SalesPerson 
   set  SalesQuota = isnull(SalesQuota,0) + isnull(@SalesQuota,0),
        Bonus = isnull(Bonus,0) + isnull(@Bonus,0),
        SalesYTD = isnull(SalesYTD,0) + isnull(@SalesYTD,0),
        SalesLastYear = isnull(SalesLastYear,0) + isnull(@SalesLastYear,0)
   where BusinessEntityID=@EmployeeID_MergeTo 

   delete from Sales.SalesPerson where BusinessEntityID=@EmployeeID_MergeFrom 

   --- Table 8: EmailAddress
   delete from Person.EmailAddress  where BusinessEntityID=@EmployeeID_MergeFrom 

   
   --- Table 9: Employee: set MergeFrom inactive (Can't delete due to a trigger)
   update HumanResources.Employee set CurrentFlag=0 where BusinessEntityID=@EmployeeID_MergeFrom 

   -- For testing purpose: raising an error (violation of FK_Employee_Person_BusinessEntityID) to check error handling and transaction rollback  
   -- delete from Person.Person where BusinessEntityID=@EmployeeID_MergeFrom 

  if @@trancount > 0
	  begin commit transaction T1 
	  end
  end try

  begin catch
	
	 declare @ErrMsg nvarchar(3000), @ErrSeverity int, @ErrState int
	 set @ErrMsg = 'Error occurred in spMergeSalesPersonIntoAnotherSalesPerson - ' + char(13) + ' Details: ' + error_message()
     set @ErrSeverity = error_severity() 
     set @ErrState = error_state()	

	 if  @@trancount > 0
        begin rollback transaction T1
	 end
	   
     --print @ErrMsg
	 --print error_message()
	 raiserror (@ErrMsg, @ErrSeverity, @ErrState)  

  end catch 

end

grant exec on dbo.spMergeSalesPeople to public


/* ---- script to execute stored procedure to merge 
exec dbo.spMergeSalesPeople
        @EmployeeID_MergeFrom=276,
        @EmployeeID_MergeTo=288,
        @AddressLine1='Pascalstr 951',
		@AddressLine2=null,
        @City='Berlin',
        @StateProvinceID=20,
        @PostalCode='14111',
        @SpatialLocation='0xE6100000010C078D021125484A408AD730111F9C2A40',
        @AddressTypeID_MergeTo=2
*/


