using System;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;

namespace AwsDotnetCsharp
{
    public class Handler
    {
        AwsRegion _region = AwsRegion.USEast1;
        AwsCredentials _credentials = new AwsCredentials("", "");
        const string BucketName = "counter-as-a-service";

        public async Task<RegisterResponse> RegisterAsync(RegisterRequest request)
        {
            var client = new S3Client(_region, _credentials);
            var ret = new RegisterResponse
            {
                PublicKey = Guid.NewGuid().ToString(),
                PrivateKey = Guid.NewGuid().ToString()
            };

            await client.PutObject(new PutObjectRequest(_region, BucketName, $"{ret.PublicKey}/.private")
            {
                Content = new StringContent(ret.PrivateKey)
            });

            await client.PutObject(new PutObjectRequest(_region, BucketName, $"{ret.PublicKey}/.email")
            {
                Content = new StringContent(request.Email)
            });

            return ret;
        }

        public async Task<long> IncrementAsync(IncrementRequest request)
        {
            var client = new S3Client(_region, _credentials);

            try
            {
                var obj = await client.GetObject(new GetObjectRequest(_region, BucketName, $"{request.PublicKey}/.private"));
                using (var stream = await obj.OpenAsync())
                using (var reader = new StreamReader(stream))
                {
                    if (await reader.ReadToEndAsync() != request.PrivateKey)
                    {
                        throw new InvalidOperationException();
                    }
                }
            }
            catch
            {
                throw new InvalidOperationException();
            }

            var key = $"{request.PublicKey}/{request.Key}";
            try
            {
                var obj = await client.GetObject(new GetObjectRequest(_region, BucketName, key));
                using (var stream = await obj.OpenAsync())
                using (var reader = new StreamReader(stream))
                {
                    var current = long.Parse(await reader.ReadToEndAsync());

                    await client.PutObject(new PutObjectRequest(_region, BucketName, key)
                    {
                        Content = new StringContent((current + 1).ToString())
                    });

                    return current + 1;
                }
            }
            catch
            {
                await client.PutObject(new PutObjectRequest(_region, BucketName, key)
                {
                    Content = new StringContent("1")
                });

                return 1;
            }
        }

        public async Task<long> GetAsync(GetRequest request)
        {
            var client = new S3Client(_region, _credentials);
            var key = $"{request.PublicKey}/{request.Key}";

            try
            {
                var obj = await client.GetObject(new GetObjectRequest(_region, BucketName, key));
                using (var stream = await obj.OpenAsync())
                using (var reader = new StreamReader(stream))
                {
                    return long.Parse(await reader.ReadToEndAsync());
                }
            }
            catch
            {
                return 0;
            }
        }
    }

    public class RegisterRequest
    {
        public string Email { get; set; }
    }

    public class RegisterResponse
    {
        public string PublicKey { get; set; }
        public string PrivateKey { get; set; }
    }

    public class IncrementRequest
    {
        public string PublicKey { get; set; }
        public string PrivateKey { get; set; }
        public string Key { get; set; }
    }

    public class GetRequest
    {
        public string PublicKey { get; set; }
        public string Key { get; set; }
    }
}
