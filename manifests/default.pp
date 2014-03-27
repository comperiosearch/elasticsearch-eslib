user {'htb': 
	ensure => present,
}

class { 'java':
  distribution => 'jdk',
  version      => 'latest',
}

class environ (   $java_home = "/usr/lib/jvm/java/")
{  
	
	  file { "/etc/profile.d/java.sh":
	  require =>  Class['java'],
      content => "export JAVA_HOME=${java_home}
                  export PATH=\$PATH:\$JAVA_HOME/bin"
  }
  
}

class { 'elasticsearch':
	require => Class['java'],
	service_provider => 'init',
   package_url => 'https://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.1.0.noarch.rpm',
   config                   => {

   	'cluster'				=> {
   		'name'				=> 'Mycluster'
   	},	
     'node'                 => {
       'name'               => 'SchoobyDoo'
     },
     'index'                => {
       'number_of_replicas' => '0',
       'number_of_shards'   => '1'
     },
     'network'              => {
       'host'               => '0.0.0.0'
     }
   }
 }