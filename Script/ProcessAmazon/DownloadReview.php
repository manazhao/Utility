<?php 
$url = "http://www.amazon.com/ss/customer-reviews/ajax/reviews/get/";
$ch = curl_init($url);
curl_setopt($ch, CURLOPT_USERAGENT,"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/33.0.1750.152 Safari/537.36");
curl_setopt($ch, CURLOPT_POST,true);
curl_setopt($ch, CURLOPT_RETURNTRANSFER,true);

# open standard input for eading
$fp = fopen("php://stdin","r");

while(($line = fgets($fp)) != null){
	# remove new line
	$line = trim($line);
	list($asin,$num_pages) = explode(",",$line);
	for($i =1; $i <= $num_pages; $i++){
		$offset = ($i - 1) * 10;
		$post_field_str = "asin=$asin&reviewerType=all_reviews&filterByStar=all_stars&formatType=all_formats&sortBy=helpful&offset=$offset&count=10";
		curl_setopt($ch, CURLOPT_POSTFIELDS,$post_field_str);
		$response = curl_exec($ch);
		if($response){
			$file = "$asin" . "_$i";
			file_put_contents($file,$response);
		}
		
	}
}
curl_close($ch);


?>
