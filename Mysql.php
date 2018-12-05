<?php /* Copyright (C) 2017 David O'Riva. MIT License.
       ********************************************************/

namespace starekrow\Toolbox;

/*
================================================================================

Mysql

Represents a connection to a MySQL database. Requires PDO.

================================================================================
*/

class Mysql
{
    public $connected;				// current best guess
    public $readOnly;

	/*
	=====================
	quoteString
	=====================
	*/
	function quoteString( $str )
	{
		return $this->db->quote( $str );
	}

	/*
	=====================
	quoteName
	=====================
	*/
	function quoteName( $name )
	{
		if ($name === '*') {
			return $name;
		}
	    return "`" . str_replace("`", "``", $name) . "`";
	}

    /*
	=====================
	assocToObject

	Converts an array to an object, only if it is not a strictly linear array.
	=====================
	*/
	static function assocToObject($arr)
	{
	    if (is_array( $arr ) && array() !== $arr) {
			if (array_keys($arr) !== range(0, count($arr) - 1)) {
				return (object)$arr;
			}
		}
		return $arr;
    }


    /*
	=====================
	is_assoc
	=====================
	*/
	static function is_assoc($arr)
	{
	    if (!is_array( $arr ) || array() === $arr) {
	    	return false;
	    }
	    return array_keys($arr) !== range(0, count($arr) - 1);
    }
    
	protected $db;
	public $errorCode;
	public $error;

	protected static $autoInstance;
	protected static $autoConfig;

	/*
	=====================
	AutoConnect
	=====================
	*/
	public static function AutoConnect( $instance = null )
	{
		if ($instance) {
			self::$autoInstance = $instance;
		} else if (self::$autoInstance && self::$autoInstance->connected) {
			return self::$autoInstance;
		}
		if (!$instance) {
			$instance = self::$autoInstance = new Sql();
		}
		if (!$instance->connected && self::$autoConfig) {
			$c = self::$autoConfig;
			if (!$instance->Connect( 
				 $c[ 'username' ]
				,$c[ 'password' ]
				,$c[ 'host' ]
				,$c[ 'port' ]
			)) {
				error_log( "DB Connection failed" );
				return $instance;
			}
			if ($c[ 'database' ]) {
				$instance->UseDatabase( $c[ 'database' ] );
			}
			// do everything in UTC
			$instance->RunQuery( "SET time_zone='+00:00'" );
		}
		return $instance;
	}

	/*
	=====================
	AutoConfig
	Sets parameters to be used in a future call to AutoConnect
	=====================
	*/
	public static function AutoConfig( $config )
	{
		if (!$config) {
			self::$autoConfig = null;
			return;
		}
		self::$autoConfig = [
			 'username' => ''
			,'password' => ''
			,'host' => null
			,'port' => null
			,'database' => null
		];
		foreach ($config as $k => $v) {
			self::$autoConfig[ $k ] = $v;
		}
	}

	/*
	=====================
	connect
	=====================
	*/
	function connect( $user, $password, $host = null, $port = null )
	{
		global $db_obj;
		$this->clearErrors();
		try {
			if (!$host) {
				$host = "localhost";
			}
			if (!$port) {
				$port = 3306;
			}
			$db = new \PDO(
				"mysql:host=$host;port=$port;charset=binary",
				$user,
				$password
			);
			if (!$db) {
				return $this->setError( 79, "open failed with no explanation" );
			}
			$this->db = $db;
			$this->connected = true;
		} catch( \PDOException $e ) {
			return $this->setError( 79, $e->getMessage() );
		}
		return true;
	}

	/*
	=====================
	useDatabase
	=====================
	*/
	public function useDatabase( $dbname )
	{
		return $this->runQuery("USE " . $this->quoteName($dbname));
	}


	/*
	=====================
	setPreferredEncoding

	Indicates how you like to see strings. Some databases will automatically
	translate responses from one encoding to another for you.
	=====================
	*/
	public function setPreferredEncoding( $encoding )
	{
		return $this->runQuery("SET NAMES " . $this->quoteName($encoding));
	}

	/*
	=====================
	setReadOnly
	=====================
	*/
	public function setReadOnly($readOnly)
	{
		$this->readOnly = !!$readOnly;
	}


	const Q_ROW_COUNT 		= 1;
	const Q_SINGLE 			= 2;
	const Q_ROW 			= 3;
	const Q_COLUMN 			= 4;
	const Q_INSERT_ID		= 5;
	const Q_ROWS	 		= 6;
	const Q_TYPE_MASK		= 0x00ff;
	const Q_NUMBERED 		= 0x1000;
	const Q_CACHE 			= 0x2000;

	protected $stcache = [];
	/*
	=====================
	runQuery
		returns array of associative arrays of results, or
        FALSE if there were no results or an error occurred
        
        You can always check `if ($db->error) {...` to differentiate
        error results.
	=====================
	*/
	protected function runQuery($stmt, $args = null, $type = self::Q_ROWS)
	{
		$this->clearErrors();

        if ($this->readOnly) {
            switch (strtolower(strtok($stmt," \t\r\n"))) {
            case 'select':
            case 'show':
            case 'use':
                break;
            case 'set':
                switch(strtolower(strtok(null, " \t\r\n"))) {
                case 'names':
                    break;
                default:
                    return $this->setError(-1, 'Illegal query for read-only database');
                }
                break;
            default:
                return $this->setError(-1, 'Illegal query for read-only database');
            }
        }

		$good = true;
		if (!empty( $this->stcache[ $stmt ] )) {
			$ps = $this->stcache[ $stmt ];
			$good = $ps->execute( $args );
			$s = $ps;
		} else if ($args || ($type & self::Q_CACHE)) {
			$ps = $this->db->prepare( $stmt );
			if (!$ps) {
				$this->pdoCheckError( $this->db );
				if (!$this->error) {
					$this->Error( -1, "Unable to prepare statement" ); 
				}
				return false;
			}
			if ($type & self::Q_CACHE) {
				$this->stcache[ $stmt ] = $ps;
			}
			$good = $ps->execute( $args );
			$s = $ps;
		} else {
			$s = $this->db->query( $stmt );
		}
		if ($s === false) {
			$this->pdoCheckError( $this->db );
			if (!$this->error) {
				$this->Error( -1, "Unable to run query" ); 
			}
			return false;
		} else if (!$good) {
			$this->pdoCheckError( $s );
			if ($this->error) {
				return false;			
			}
		}
		switch ($type & self::Q_TYPE_MASK) {
		case self::Q_ROW_COUNT:
			$res = $s->rowCount();
			break;
		
		case self::Q_SINGLE:
			$res = $s->fetch( \PDO::FETCH_NUM );
			if( $res === FALSE )
				return FALSE;
			$res = $res[0];
			break;
		
		case self::Q_ROWS:
			if ($type & self::Q_NUMBERED) {
				$res = $s->fetchAll( \PDO::FETCH_NUM );
			} else {
				$res = $s->fetchAll( \PDO::FETCH_OBJ );
			}
			break;

		case self::Q_ROW:
			if ($type & self::Q_NUMBERED) {
				$res = $s->fetch( \PDO::FETCH_NUM );
			} else {
				$res = $s->fetch( \PDO::FETCH_OBJ );
			}
			break;

		case self::Q_INSERT_ID:
			$res = $this->db->lastInsertId();
			break;

		default:
			return $this->setError( 72, "Unknown query type " . $type );
		}
		return $res;
	}

	/*
	=====================
	query
	returns array of objects of results, OR
	false on error
	=====================
	*/
	function query( $stmt, $args = null ) 
	{
		return $this->runQuery( $stmt, $args, self::Q_ROWS );
	}

	/*
	=====================
	queryN
	returns array of regular arrays of results, OR
	false on error
	=====================
	*/
	function queryN( $stmt, $args = null ) 
	{
		return $this->runQuery( $stmt, $args, self::Q_ROWS | self::Q_NUMBERED );
	}

	/*
	=====================
	query1
	returns the first column of the first row of results, OR
	false on error
	=====================
	*/
	function query1( $stmt, $args = null )
	{
		return $this->runQuery( $stmt, $args, self::Q_SINGLE );
	}

	/*
	=====================
	exec
	returns number of rows affected, or false if db reported an error
	=====================
	*/
	function exec( $stmt, $args = null )
	{
		return $this->runQuery( $stmt, $args, self::Q_ROW_COUNT );
	}

	/*
	=====================
	execI
	returns last insert ID, or false if db reported an error
	=====================
	*/
	function execI( $stmt, $args = null )
	{
		return $this->runQuery( $stmt, $args, self::Q_INSERT_ID );
	}

	/*
	=====================
    quoteList
	=====================
    */
    function quoteList($list)
    {
        $res = [];
        foreach ($list as $el) {
            if (is_int($el) || is_double($el)) {
                $res[] = $el;
            } else if (is_bool($el)) {
                $res[] = $el ? 1 : 0;
            } else {
                $res[] = $this->QuoteString($el);
            }
        }
        return implode(",", $res);
    }

    /*
	=====================
    quoteNameList
	=====================
    */
    function quoteNameList($list)
    {
        $res = [];
        foreach ($list as $el) {
            $res[] = $this->quoteName($el);
        }
        return implode(",", $res);
    }

    const MAX_INSERT_ROW_BATCH          = 2000;
    const MAX_INSERT_STMT_LEN           = 300000;

	/*
	=====================
	insert
	=====================
	*/
	function insert($query, $arg2)
	{
		$table = null;
		$rows = null;
		$fields = null;
		$rtype = null;
		$row = null;

		if (is_string($query)) {
			$table = $query;
			if (is_array($arg2) && isset($arg2[0]) && (is_array($arg2[0]) || is_object($arg2[0]))) {
				$rows = $arg2;
			} else {
				$row = $arg2;
			}
		} else {
			foreach ($query as $k => $v) {
				switch ($k) {
				case "row":
					$fields = $this->fields;
					if ($rows) {
						$err = "query->row conflicts";
					} else if (is_object($v) || is_array($v)) {
						$rows = [ $v ];
					} else {
						$err = "bad value in query->row";
					}
					break;
	
				case "table":
					if (is_string( $v )) {
						$table = $v;
					} else {
						$err = "Bad value for query->table";
					}
					break;
	
				case "rows":
					if ($rows) {
						$err = "query->rows conflicts";						
					} else if (is_array( $v ) && (isset($v[0]) || !count($v))) {
						$rows = $v;
					} else {
						$err = "bad value in query->rows";
					}
					break;
	
				case "fields":
					if (is_array( $v ) && isset($v[0])) {
						$fields = $v;
					} else {
						$err = "bad value in query->fields";
					}
					break;
	
				case "result":
					switch ($v) {
					case "id":
						$rtype = self::Q_INSERT_ID;
						break;
					case "count":
						$rtype = self::Q_INSERT_COUNT;
						break;
					}
					break;
	
				default:
					$err = "Unknown insert entry $k";
					break;
				}
			}	
		}
		if ($row) {
			$rows = [ $row ];
		}
		if ($rtype === null) {
			$rtype = $row ? self::Q_INSERT_ID : self::Q_INSERT_COUNT;
		}
		if (!empty($rows) && empty($fields)) {
			$v = $rows[0];
			if (is_object($v)) {
				$fields = array_keys((array)$v);
			} else if (is_array($v) && !isset($v[0])) {
				$fields = array_keys($v);
			}
		}
		if (!$table || !$rows) {
			$err = "Missing query information";
		} else if (!count($rows)) {
            return 0;
			//$err = "Empty rows list";
		} else {
			$c = count($fields);
            $rc = count($rows);
            $fieldkeys = array_flip($fields);
			$stmt = "INSERT INTO " . $this->quoteName($table) . " ";
			if ($c) {
				$stmt .= "(" . $this->quoteNameList($fields) . ") ";
			}
			$stmt .= "VALUES ";

            $out = [];
			$len = $slen = strlen($stmt) + 2;
            $inserted = 0;
			for ($i = 0; $i < $rc; ++$i) {
				$r = $rows[ $i ];
				
				if (is_object($r)) {
					$r = (array)$r;
				} else if (!is_array($r)) {
					$err = "Bad type in row $i";
					break;
				}
				if (!isset($r[0])) {
					if (count($r) == 0) {
						$err = "Empty row at $i";
						break;
					} else if (!$c) {
						$err = "Bad type in row $i";
						break;
					}
					// efficiently sorts the array values by the field keys
					$r = array_values(array_replace($fieldkeys, $r));
				}
				if ($c && count( $r ) != $c) {
					$err = "Bad length in row $i";
					break;
                }
                $vals = $this->quoteList($r);
                $len += strlen($vals) + 3;
                $out[] = $vals;
				if (count($out) > self::MAX_INSERT_ROW_COUNT 
					|| $len >= self::MAX_INSERT_STMT_LEN 
					|| $i == $rc - 1
				   ) {
                    $sql = $stmt . "(" . implode("),(", $out) . ")";
					$added = $this->runQuery($sql, null, self::Q_ROW_COUNT);
					$sql = null;
                    if ($added != count($out)) {
                        $err = "Only inserted {$did} of " . count($out) . "rows";
                        break;
                    }
                    $inserted += $added;
                    $out = [];
                    $len = $slen;
                }
			}
		}
		if ($err) {
			return $this->Error( 71, $err );
        }
        return $inserted;
	}

	/*
	=====================
	quoteColumnName
	=====================
	*/
	function quoteColumnName( $colname )
	{
		$cn = explode( '.', $colname, 2 );
		if (count( $cn ) == 1) {
			return $this->quoteName( $cn[0] );
		}
		return $this->quoteName( $cn[0] ) . '.' . $this->quoteName( $cn[1] );
	}

	/*
	=====================
	tableExists
	=====================
	*/
	function tableExists($table)
	{
		$res = $this->query1(
			"SELECT COUNT(*) FROM information_schema.tables 
			 WHERE table_schema=DATABASE() AND table_name=?",
			 $table );
		return $res == 1;
	}

	/*
	=====================
	columnExists
	=====================
	*/
	function columnExists( $table, $column )
	{
		$res = $this->query1(
			"SELECT COUNT(*) 
			   FROM information_schema.columns 
			   WHERE table_schema=DATABASE() 
			     AND table_name=? AND column_name=?",
			$table, $column );
		return $res == 1;
	}

	/*
	=====================
	clearErrors
	=====================
	*/
	function clearErrors()
	{
		$this->errorCode = 0;
		$this->error = null;
	}

	/*
	=====================
	setError
	=====================
	*/
	protected function setError( $code, $msg = false )
	{
		if (!$code) {
			$this->errorCode = -1;
			$this->error = $msg;
			return;
		}
		$this->errorCode = $code;
		$this->error = ($msg === false) ? "DB Error " . $code : $msg;
		return false;
	}

	/*
	=====================
	pdoCheckError
	=====================
	*/
	protected function pdoCheckError( $el )
	{
		$r = array( 0, 73, "Cannot get error info" );
		if (!$this->db) {
			$r = array( 0, 73, "Database not connected" );
		} else {
			try {
				$r = $el->errorInfo();
			} catch( \PDOException $e ) {}
		}
		if( count( $r ) < 3 ) {
			if( $r[0]=='00000' || substr($r,0,2)=="01" ) {
				return false;
			}
			$r = array( 0, 79, $r[0] );
		}
		$this->Error( $r[1], $r[2] );
		return true;
	}
}
