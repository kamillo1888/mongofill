<?php

use Mongofill\Protocol;
use Mongofill\Socket;

if (!defined('MONGOFILL_USE_APC')) {
    define('MONGOFILL_USE_APC', function_exists('apc_store'));
}

/**
 * A connection manager for PHP and MongoDB.
 */
class MongoClient
{
    const VERSION = '1.3.0-mongofill';
    const DEFAULT_HOST = 'localhost';
    const DEFAULT_PORT = 27017;
    const DEFAULT_DATABASE = 'admin';
    const RP_PRIMARY   = 'primary';
    const RP_PRIMARY_PREFERRED = 'primaryPreferred';
    const RP_SECONDARY = 'secondary';
    const RP_SECONDARY_PREFERRED = 'secondaryPreferred';
    const RP_NEAREST   = 'nearest';
    const RP_DEFAULT_ACCEPTABLE_LATENCY_MS = 15;

    const DEFAULT_CONNECT_TIMEOUT_MS = 60000;
    const REPL_SET_CACHE_LIFETIME = 15; // in seconds

    const STATE_STARTUP = 0;
    const STATE_PRIMARY = 1;
    const STATE_SECONDARY = 2;

    const STATE_STR_PRIMARY = 'PRIMARY';
    const STATE_STR_SECONDARY = 'SECONDARY';

    const HEALTHY = 1;

    /**
     * @var boolean
     */
    public $connected = false;

    /**
     * @var string
     */
    public $boolean = false;

    /**
     * @var string
     */
    public $status;

    /**
     * @var boolean
     */
    public $persistent;

    /**
     * @var array
     */
    private $options;

    /**
     * @var string
     */
    private $username;

    /**
     * @var string
     */
    private $password;

    /**
     * @var string
     */
    private $database;

    /**
     * @var string
     */
    private $uri;

    /**
     * @var array<string>
     */
    private $hosts = [];

    /**
     * @var array<Protocol>
     */
    public $protocols = [];

    /**
     * @var array<Socket>
     */
    private $sockets = [];

    /**
     * @var array
     */
    private $databases = [];

    /**
     * @var string
     */
    private $replSet;

    /**
     * @var array
     */
    private $replSetStatus = [];

    /**
     * @var array
     */
    private $replSetConf = [];

    /**
     * @var array
     */
    private $readPreference = ['type' => self::RP_PRIMARY];

    /**
     * @var int
     */
    private $connectTimeoutMS;

    /**
     * Holds a reference to the PRIMARY connection for replSet connections once it
     * has been established
     *
     * @var Mongofill\Protocol
     */
    private $primaryProtocol;


    /**
     * @var \MongoDB\Client|null
     */
    public $mongoDBClient = null;

    public $mongoCollections = [];

    public function getCurrentClient()
    {
        return $this->mongoDBClient;
    }

    /**
     * Creates a new database connection object
     *
     * @param string $server - The server name. server should have the form:
     *      mongodb://[username:password@]host1[:port1][,host2[:port2:],...]/db
     * @param array $options - An array of options for the connection.
     * @throws MongoConnectionException
     */
    public function __construct($server = 'mongodb://localhost:27017', array $options = ['connect' => true])
    {
        // Step first - connect to database
        if (true === is_null($this->mongoDBClient)) {
            $this->mongoDBClient = new \MongoDB\Driver\Manager($server, $options);
        }
    }

    /**
     * Closes this connection
     *
     * @param boolean|string $connection - If connection is not given, or
     *   FALSE then connection that would be selected for writes would be
     *   closed. In a single-node configuration, that is then the whole
     *   connection, but if you are connected to a replica set, close() will
     *   only close the connection to the primary server.
     *
     * @return bool - Returns if the connection was successfully closed.
     */
    public function close($connection = null)
    {
        throw new Exception('Not Implemented');
    }

    /**
     * Connects to a database server or replica set
     *
     * @return bool - If the connection was successful.
     * @throws MongoConnectionException
     */
    public function connect()
    {
        return $this->connected = true;
    }

    /**
     * Drops a database [deprecated]
     *
     * @param mixed $db - The database to drop. Can be a MongoDB object or
     *   the name of the database.
     *
     * @return array - Returns the database response.
     */
    public function dropDB($db)
    {
        throw new Exception('Not Implemented');
    }

    /**
     * Gets a database
     *
     * @param string $dbname - The database name.
     *
     * @return MongoDB - Returns a new db object.
     */
    public function __get($dbname)
    {
        return $this->selectDB($dbname);
    }

    /**
     * Return info about all open connections
     *
     * @return array - An array of open connections.
     */
    public static function getConnections()
    {
        throw new Exception('Not Implemented');
    }

    /**
     * Updates status for all associated hosts
     *
     * @return array - Returns an array of information about the hosts in
     *   the set. Includes each host's hostname, its health (1 is healthy),
     *   its state (1 is primary, 2 is secondary, 0 is anything else), the
     *   amount of time it took to ping the server, and when the last ping
     *   occurred.
     * @throws Exception
     */
    public function getHosts()
    {
        return $this->hosts;
    }

    /**
     * Get the read preference for this connection
     *
     * @return array -
     */
    public function getReadPreference()
    {
        return $this->readPreference;
    }

    /**
     * Kills a specific cursor on the server
     *
     * @param string $serverHash - The server hash that has the cursor.
     *   This can be obtained through MongoCursor::info.
     * @param int|mongoint64 $id - The ID of the cursor to kill. You can
     *   either supply an int containing the 64 bit cursor ID, or an object
     *   of the MongoInt64 class. The latter is necessary on 32 bit platforms
     *   (and Windows).
     *
     * @return bool - Returns TRUE if the method attempted to kill a
     *   cursor, and FALSE if there was something wrong with the arguments
     *   (such as a wrong server_hash). The return status does not reflect
     *   where the cursor was actually killed as the server does not provide
     *   that information.
     */
    public function killCursor($serverHash, $id)
    {
//        // since we currently support just single server connection,
//        // the $serverHash arg is ignored
//
//        if ($id instanceof MongoInt64) {
//            $id = $id->value;
//        } elseif (!is_numeric($id)) {
//            return false;
//        }
//
//        $this->protocols[$serverHash]->opKillCursors([ (int)$id ], [], MongoCursor::$timeout);

        return true;
    }

    /**
     * Lists all of the databases available.
     *
     * @return array - Returns an associative array containing three
     *   fields. The first field is databases, which in turn contains an
     *   array. Each element of the array is an associative array
     *   corresponding to a database, giving th database's name, size, and if
     *   it's empty. The other two fields are totalSize (in bytes) and ok,
     *   which is 1 if this method ran successfully.
     */
    public function listDBs()
    {
        $cmd = [
            'listDatabases' => 1
        ];

        $result = $this->selectDB(self::DEFAULT_DATABASE)->command($cmd);

        return $result;
    }

    /**
     * Gets a database collection
     *
     * @param string $db - The database name.
     * @param string $collection - The collection name.
     *
     * @return MongoCollection - Returns a new collection object.
     */
    public function selectCollection($db, $collection)
    {
        var_dump(__METHOD__);
        die();
//        dump("COLLECTION SELECT", $db, $collection);
//        return $this->selectDB($db)->selectCollection($collection);
    }

    /**
     * Gets a database
     *
     * @param string $name - The database name.
     *
     * @return MongoDB - Returns a new database object.
     */
    public function selectDB($name)
    {
        if (!isset($this->databases[$name])) {
            $this->databases[$name] = new MongoDB($this, $name);
        }

        return $this->databases[$name];
    }

    /**
     * Set the read preference for this connection
     *
     * @param string $readPreference -
     * @param array $tags -
     *
     * @return bool -
     */
    public function setReadPreference($readPreference, array $tags = null)
    {
//        if ($new_preference = self::_validateReadPreference($readPreference, $tags)) {
//            $this->readPreference = $new_preference;
//        }
//        return (bool)$new_preference;
    }


    /**
     * String representation of this connection
     *
     * @return string - Returns hostname and port for this connection.
     */
    public function __toString()
    {
        $first_host = reset($this->hosts);
        return $first_host['host'] . ':' . $first_host['port'];
    }
}
