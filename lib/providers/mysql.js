/**
 * mysql specific implementation.
 *
 * @module mysql
 *
 * @copyright 2014, Digium, Inc.
 * @license Apache License, Version 2.0
 * @author Andrew Nagy <anagy@sangoma.com>
 */

'use strict';

var mysql = require('mysql');
var Q = require('q');
var moment = require('moment');
var util = require('util');

console.log("LOADED2");
/**
 * Returns an API for mysql specific database operations.
 *
 * @param {Object} config - config object containing connection string and
 *                          provider name
 * @param {object} dependencies - object keyed by module dependencies
 */
function createApi(config, dependencies) {
  return {
    /*
     * Runs the given query inside a transaction.
     *
     * @param {Query} query - node-sql query object
     * @returns {Q} promise - a promise containing the result of the query
     */
    runQuery: function(query) {
      dependencies.logger.trace('mysql.runQuery called');

      var connection = mysql.createConnection({
      	host     : 'localhost',
      	user     : 'freepbxuser',
      	password : '9defd4b6cedf3985089abcbbe247931e',
      	database : 'asterisk'
      });

      var connect = Q.denodeify(connection.connect.bind(connection));
      return connect().then(function(con) {
      	var clientQuery = Q.denodeify(connection.query.bind(connection));
        return clientQuery('START TRANSACTION')
          .then(function () {
            dependencies.logger.debug({
              query: query
            }, 'Running query');
            return clientQuery(query.text, query.values);
          })
          .then(function (result) {
            dependencies.logger.debug('Committing');

            return clientQuery('COMMIT')
              .then(function () {
                return result;
              });
          })
          .catch(function (error) {
            dependencies.logger.debug('Rolling back');

            return clientQuery('ROLLBACK')
              .then(function() {
                throw new Error(error);
              });
          })
          .finally(function() {
            connection.end();
          });
      }).catch(function (error) {
      	throw new Error(error);
      });
    },

    /*
     * Begins a transaction.
     *
     * @param {bool} lock - whether to lock, does not apply to mysql
     * @returns {Q} promise - a promise containing functions to run queries
     * and commit/rollback the transaction
     */
    beginTransaction: function(lock) {
      dependencies.logger.trace('mysql.beginTransaction called');

      var connection = mysql.createConnection({
      	host     : 'localhost',
      	user     : 'freepbxuser',
      	password : '9defd4b6cedf3985089abcbbe247931e',
      	database : 'asterisk'
      });

      var connect = Q.denodeify(connection.connect.bind(connection));
      var clientQuery;
      var done = connection.end();

      return connect().then(function(con) {
      	clientQuery = Q.denodeify(connection.query.bind(connection));
        return clientQuery('START TRANSACTION')
          .then(function () {
            return {
              commit: commitTransaction,
              rollback: rollbackTransaction,
              runQuery: runQueryWithoutTransaction
            };
          });
      });

      /**
       * Commits the transaction and releases the connection.
       *
       * @returns {Q} promise - a promise containing the result of committing
       */
      function commitTransaction() {
        dependencies.logger.trace('mysql.trans.commit called');
        return clientQuery('COMMIT')
          .finally(function() {
            done();
          });
      }

      /**
       * Rolls back the transaction and releases the connection.
       *
       * @returns {Q} promise - a promise containing the result of rolling
       *                        back
       */
      function rollbackTransaction() {
        dependencies.logger.trace('mysql.trans.rollback called');

        return clientQuery('ROLLBACK')
          .finally(function() {
            done();
          });
      }

      /**
       * Returns a promise containing the result of the query.
       *
       * @param {Query} query - node-sql query object
       * @returns {Q} promise - a promise containing the result of running
       *   the query
       */
      function runQueryWithoutTransaction(query) {
        dependencies.logger.trace('mysql.trans.runQuery called');

        dependencies.logger.debug({
          query: query
        }, 'Running query');

        return clientQuery(query.text, query.values)
          .then(function (result) {
            return result;
          });
      }
    },

    /**
     * Adds for update to a select statement for row locking.
     *
     * @param {object} query - node-sql query object
     * @returns {object} query - a new query with a for update statement
     */
    forUpdate: function(query) {
      dependencies.logger.trace('mysql.forUpdate called');

      var replaced = {};
      replaced.text = util.format('%s FOR UPDATE', query.text);
      replaced.values = query.values;

      return replaced;
    },

    /**
     * Create statement does not have to be modified for mysql. Integer primary
     * keys automatically get a unique Integer value if not given on insert.
     *
     * @param {string} createStatement - the create statement to modify
     */
    autoIncrement: function(createStatement) {
      dependencies.logger.trace('mysql.autoIncrement called');

      return createStatement;
    },

    /**
     * Modifies date from db storage to object format.
     *
     * @param {Object} date - date object fetched from database
     * @returns {Moment} date - Moment date object
     */
    convertDateFromStorage: function(date) {
      dependencies.logger.trace('mysql.convertDateFromStorage called');

      return moment.unix(date).utc();
    },

    /**
     * Modifies date from object format for db storage.
     */
    convertDateForStorage: function(date) {
      dependencies.logger.trace('mysql.convertDateForStorage called');

      return date.unix();
    },

    /**
     * Returns the date type for table creation.
     */
    getDateType: function() {
      dependencies.logger.trace('mysql.getDateType called');

      return 'timestamp';
    }
  };
}

/**
 * Returns a mysql provider helper object.
 *
 * @param {Object} config - config object containing connection string and
 *                          provider name
 * @param {object} dependencies - object keyed by module dependencies
 */
module.exports = function(config, dependencies) {
  var obj = createApi(config, dependencies);
  obj.overrides = {
    mailbox: {},
    context: {},
    folder: {},
    mailboxConfig: {},
    contextConfig: {},
    message: {}
  };

  return obj;
};
