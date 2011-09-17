<?php
declare(ticks = 1);

$server = new Http('10.245.207.200', 80);

$map = new DHT();

$server->listen(function($request,$response) use($map) {
	$map->set('hello','world');
	$response->write($map->get('hello'));
});


class Tcp {

	public function __construct($host, $port, $parent = null) {
		$this->host = $host;
		$this->port = $port;
		$this->ppid = $parent;
	}
	
	public function listen($fn) {
	
		if($this->_open() && $this->ppid) {
			posix_kill($this->ppid,SIGUSR1);
		}
		
		while(true) {
			$this->client = @socket_accept($this->socket);
			if($this->client !== false) {
				$pid = pcntl_fork();
				
				if ($pid === -1){
					throw new Exception("Could not fork.");
				}
				
				if ($pid){
					pcntl_waitpid(-1, $status, WNOHANG);
				} else {
					$this->_callback($fn);
					$this->_close();
				}
			}
		}
	}
	
	public function read($length = 1024){
		return socket_read($this->client, $length, PHP_BINARY_READ);
	}
	
	public function write($data){
		return socket_write($this->client, $data);
	}
	
	private function _open() {
		$this->socket = socket_create(AF_INET,SOCK_STREAM,SOL_TCP);
		//socket_set_nonblock($this->socket);
		if(!$this->socket) {
			throw new Exception('Could not create socket.');
		}
		
	    if(socket_set_option($this->socket, SOL_SOCKET, SO_REUSEADDR, 1) === false){
	    	throw new Exception('Could not set reusable sockets.');
	    }
		
		if(socket_bind($this->socket, $this->host, $this->port) === false) {
			throw new Exception('Could not bind socket.');
		} 
		
		if(socket_listen($this->socket) === false) {
	    	throw new Exception('Could not listen.');
		} else {
			return true;
		}
	}
	
	private function _close() {
    	socket_shutdown($this->client);
    	socket_close($this->client);
	}
	
	public function _callback($fn) {
		call_user_func($fn,$this);
	}
	
}

class Http extends Tcp {

	public function _callback($fn) {
		$request = new Request($this);
		$response = new Response($this);
		
		call_user_func_array($fn,array($request,$response));
	}
	
}

class Request {
	
	public function __construct(Http $server) {
		$this->headers = $this->parseHeaders($server->read());
	}
	
	public function parseHeaders($headers) {
        $retVal = array();
        $fields = explode("\r\n", preg_replace('/\x0D\x0A[\x09\x20]+/', ' ', $headers));
        foreach( $fields as $field ) {
            if( preg_match('/([^:]+): (.+)/m', $field, $match) ) {
                $match[1] = preg_replace('/(?<=^|[\x09\x20\x2D])./e', 'strtoupper("\0")', strtolower(trim($match[1])));
                if( isset($retVal[$match[1]]) ) {
                    $retVal[$match[1]] = array($retVal[$match[1]], $match[2]);
                } else {
                    $retVal[$match[1]] = trim($match[2]);
                }
            }
        }
        return $retVal;
	} 
	
}

class Response {

	private $status = 200;
	private $headersSent = false;
	private $headers = array(
		'Content-Type' => 'text/html'
	);

	private $statusStrings = array(
		100 => 'Continue',
		101 => 'Switching Protocols',
		200 => 'OK',
		201 => 'Created',
		202 => 'Accepted',
		203 => 'Non-Authoritative Information',
		204 => 'No Content',
		205 => 'Reset Content',
		206 => 'Partial Content',
		300 => 'Multiple Choices',
		301 => 'Moved Permanently',
		302 => 'Found',
		303 => 'See Other',
		304 => 'Not Modified',
		305 => 'Use Proxy',
		307 => 'Temporary Redirect',
		400 => 'Bad Request',
		401 => 'Unauthorized',
		402 => 'Payment Required',
		403 => 'Forbidden',
		404 => 'Not Found',
		405 => 'Method Not Allowed',
		406 => 'Not Acceptable',
		407 => 'Proxy Authentication Required',
		408 => 'Request Timeout',
		409 => 'Conflict',
		410 => 'Gone',
		411 => 'Length Required',
		412 => 'Precondition Failed',
		413 => 'Request Entity Too Large',
		414 => 'Request-URI Too Long',
		415 => 'Unsupported Media Type',
		416 => 'Requested Range Not Satisfiable',
		417 => 'Expectation Failed',
		500 => 'Internal Server Error',
		501 => 'Not Implemented',
		502 => 'Bad Gateway',
		503 => 'Service Unavailable',
		504 => 'Gateway Timeout',
		505 => 'HTTP Version Not Supported'
	);

	public function __construct(Http $server) {
		$this->server = $server;
	}
	
	public function write($data) {
		if ($this->headersSent === false){
			$this->sendHeaders();
		}
		return $this->server->write($data);
	}
	
	public function setStatus($code) {
		if (!isSet($this->statusStrings[$code])) {
			throw new Exception("[{$code}] is not a recognised HTTP status code");
		}
		$this->status = $code;
		return $this;
	}
	
	public function setHeader($name, $value) {
		$this->headers[$name] = $value;
		return $this;
	}
	
	public function setHeaders(array $headers) {
		foreach ($headers as $name => $value){
			$this->setHeader($name, $value);
		}
			return $this;
		}
	
	public function sendHeaders(array $headers = array()) {
		if ($this->headersSent) {
			return false;
		} else {
			$this->setHeaders($headers);
	
			$statusText = $this->statusStrings[$this->status];
			$headerText = "HTTP/1.1 {$this->status} {$statusText}\r\n";
	
			foreach ($this->headers as $name => $value){
				$headerText .= "{$name}: {$value}\r\n";
			}
			
			$headerText .= "\r\n";
			$this->headersSent = true;
			
			return $this->server->write($headerText);
		}

  }

}

class SharedMemory {

	public function __construct($size = 10000) {
		$tmp = tempnam('/tmp', 'PHP');
		$this->key = ftok($tmp, 'a');
		$this->id = shm_attach($this->key,$size);
		
		if ($this->id === false) {
		    throw new Exception('Unable to create the shared memory segment.');
		} else {
			$index = array();
			shm_put_var($this->id, 0, $index);
		}
	}
	
	public function __destruct() {
		shm_remove($this->id);
		shm_detach($this->id);
	}
	
	public function get($key) {
		$key = $this->_map($key);
		return shm_get_var($this->id,$key);
	}
	
	public function set($key,$val) {
		$index = shm_get_var($this->id,0);
		$index[$key] = count($index) + 1;
		shm_put_var($this->id,0,$index);
		shm_put_var($this->id,$index[$key],$val);
	}
	
	public function remove($key) {
		$key = $this->_map($key);
		shm_remove_var($key);
	}
	
	private function _map($key) {
		$index = shm_get_var($this->id,0);
		return (int) $index[$key];
	}

}

class HashMap {
	
	private $values = array();
	private $keys = array();

	public function get($key) {
		$hash = $this->_hash($key);
		if(!array_key_exists($hash,$this->keys)) {
			throw new Exception("Undefined index called in hashmap.");
		}
		return $this->values[$hash];
	}
	
	public function set($key,$value) {
		$hash = $this->_hash($key);
		$this->values[$hash] = $value;
		$this->keys[$hash] = $key;
	}
	
	public function count() {
		return count($this->keys);
	}
	
	private function _hash($value) {
		return md5($value);
	}
	
}

class NodeServer extends Tcp {
	
	public function __construct($host,$port,$parent) {
	
		parent::__construct($host, $port, $parent);
		$this->hashMap = new SharedMemory();
		
		$this->listen(function($server) {
			$request = $server->read();
			$request = json_decode($request);
			if($request->method == 'get') {
				$server->write($server->hashMap->get($request->key));			
			}
			if($request->method == 'set') {
				$server->hashMap->set($request->key,$request->value);
			}
		});
	}
	
}

class NodeClient {

	public function __construct($host,$port) {
		$this->host = $host;
		$this->port = $port;
	}
	
	public function get($key) {
		$request['method'] = 'get';
		$request['key'] = $key;
		return $this->_send($request);
	}
	
	public function set($key,$value) {
		$request['method'] = 'set';
		$request['key'] = $key;
		$request['value'] = $value;
		$this->_send($request);
	}

	private function _send($request) {
		$socket = socket_create(AF_INET,SOCK_STREAM,SOL_TCP);
		socket_connect($socket,$this->host,$this->port);
		socket_write($socket,json_encode($request));
		socket_recv($socket, $buffer, 2048, MSG_WAITALL);
		socket_close($socket);
		//var_dump($buffer);
		return $buffer;	
	}

}

class Node extends NodeClient {

	public function __construct($host,$port) {
		
		parent::__construct($host,$port);
		$this->ready = false;
		
		pcntl_signal(SIGUSR1, array($this,'_signal_callback'));
		$ppid = posix_getpid();
		$pid = pcntl_fork();
		
		if ($pid === -1){
			throw new Exception("Could not fork.");
		}
		
		if ($pid){
			while(!$this->ready) {
				pcntl_waitpid(-1, $status, WNOHANG);
			}
		} else {
			$server = new NodeServer($host,$port,$ppid);
		}
	}
	
	private function _signal_callback($signo, $pid=null, $status=null) {
		if($signo == SIGUSR1) {
			$this->ready = true;
		}
	}
	
}

class DHT {
	
	public function __construct() {
		$this->partitions = new HashMap();
		$this->nodes = array();
		$this->nodes[] = array(new Node('127.0.0.1',3306));
	}
	
	public function get($key) {		
		$node = $this->partitions->get($key);
		$replicaKey = array_rand($this->nodes[$node]);
		$node = $this->nodes[$node][$replicaKey];
		return $node->get($key);
	}
	
	public function set($key,$value) {
		$node = array_rand($this->nodes);
		$this->partitions->set($key,$node);
		foreach($this->nodes[$node] as $node) {
			$node->set($key,$value);
		}
	}
	
}

?>