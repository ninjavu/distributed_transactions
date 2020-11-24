require 'active_record'
require 'pg'

def connect(db_name)
  {
    adapter:  'postgresql',
    host:     'localhost',
    database: db_name
  }
end

MonolithDB = connect('M_database')
ServiceADB = connect('A_database')
ServiceBDB = connect('B_database')

module Monolith
  class TransactionsCoordinator
    # Here we need to think about:
    #
    # 1. Duplications in services array.
    # 2. TransactionsCoordinator recovery.
    # 3. Communication with monolith and microservices (Abstract layer).
    # 4. 

    def self.create(name)
      Transaction.create(id: 2, name: name)
    end

    def self.update(transaction:, service:, state:)
      transaction = Transaction.find_by_name(transaction)
       
      if transaction.status == :rollbacked
        # If we already made a rollback, just notifing other services to 
        # make revert

        return false
      end

      unless state
        # ROLLBACK DIRECTLY FROM THEIR DBs or by means of interface.

        transaction.services&.each do |service|
          # ROLLBACK IN SERVICES!
        end

        transaction.status = :rollbacked
      end

      transaction.services = []
      transaction.services << service
      transaction.state = state

      if transaction.save
        p transaction
      end
    end

    def self.success?(uid)
      # Think about race condition
      transaction = Transaction.find(uid)
      transaction.state
    end
  end
end

module Monolith
  class Transaction < ActiveRecord::Base
    establish_connection MonolithDB
  end

  class Profile < ActiveRecord::Base
    establish_connection MonolithDB

    def self.call
      transaction do
        TransactionsCoordinator.create('PWv2')
        
        # Repositories calls:
        ServiceA::Profile.call(transaction: 'PWv2')
        ServiceB::Profile.call(transaction: 'PWv2')
        
        # Preferable to change with preapared transaction too.
        raise ActiveRecord::Rollback unless TransactionsCoordinator.success?(transaction: 'PWv2')
      end
    end
  end
end

module ServiceA
  class Profile < ActiveRecord::Base
    establish_connection ServiceADB
    
    def self.call(transaction:)
      # This transaction should be rollbacked due to NOT NULL constraint

      sql = %{
        BEGIN;
        INSERT INTO profiles (firstName) VALUES (NULL);
        PREPARE TRANSACTION '#{transaction}';
      }

      begin
        connection.execute(sql)

        state = true
      rescue StandardError => e
        state = false
      ensure
        connection.close

        Monolith::TransactionsCoordinator.update(
          transaction: transaction,
          service: self.name,
          state: state
        )
      end
    end
  end
end

module ServiceB
  class Profile < ActiveRecord::Base
    establish_connection ServiceBDB

    def self.call(transaction:)
      # This transaction should be rollbacked due to NOT NULL constraint

      sql = %{
        BEGIN;
        INSERT INTO profiles (firstName) VALUES ('fsdfdf');
        PREPARE TRANSACTION '#{transaction}';
      }

      begin
        connection.execute(sql)

        state = true
      rescue StandardError => e
        state = false
      ensure
        connection.close

        Monolith::TransactionsCoordinator.update(
          transaction: transaction,
          service: self.name,
          state: state
        )
      end
    end
  end
end

Monolith::Transaction.destroy_all

p Monolith::Profile.count
p ServiceA::Profile.count
p ServiceB::Profile.count

Monolith::Profile.call

p Monolith::Profile.count
p ServiceA::Profile.count
p ServiceB::Profile.count

# In order to get around this problem, transaction will emulate the effect of nested transactions, by using savepoints: dev.mysql.com/doc/refman/5.7/en/savepoint.html Savepoints are supported by MySQL and PostgreSQL. SQLite3 version >= '3.6.8' supports savepoints.