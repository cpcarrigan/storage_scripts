#!/bin/bash

objectName="lowres_78220/rubDEG9cgXFh0zxSR6qwZA_---3AYTPbv_SS5zozYrNC8y4H6Ukwe_s0J2lFeeFX9s.jpg"
file=test.jpg
bucket=snapfish
verb="DELETE"
resource="/${bucket}/${objectName}"
contentType="image/jpeg"
# dateValue=`date -R`
dateValue='Fri, 03 Jan 2020 16:20:26 -0800'
stringToSign="${verb}\n\n${contentType}\n${dateValue}\n${resource}"
s3Key='tiEHBOJaJLFC0MDwfs5k'
echo "s3Key: '${s3Key}'"
s3Secret='hNnKKEdpZ8IKfFFsMQgcNFfKAdynoTh5PndRzJ8l'
echo "s3Secret: '${s3Secret}'"
signature=`echo -en ${stringToSign} | openssl sha1 -hmac ${s3Secret} -binary | base64`
echo "signature: '${signature}'"
#curl -k -v -i -X "${verb}" -T "${file}" \
#    -H "Host: esthreecos1.sf-cdn.com"\
#    -H "Date: ${dateValue}" \
#    -H "Content-Type: ${contentType}" \
#    -H "Authorization: AWS ${s3Key}:${signature}" \
#    https://esthreecos1.sf-cdn.com/${bucket}/${objectName}
