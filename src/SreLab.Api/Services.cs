using System.Text.Json; using Confluent.Kafka;
namespace SreLab.Api;
public class KafkaProducer : IKafkaProducer {
  private readonly string _bs; public KafkaProducer(IConfiguration c){ _bs=c["Kafka:BootstrapServers"]??"kafka:29092"; }
  public async Task PublishAsync(string topic, object payload){ var cfg=new ProducerConfig{ BootstrapServers=_bs, Acks=Acks.All }; using var p=new ProducerBuilder<string,string>(cfg).Build(); var r=await p.ProduceAsync(topic, new Message<string,string>{ Key=Guid.NewGuid().ToString(), Value=JsonSerializer.Serialize(payload)}); Console.WriteLine($"[Apache Kafka] {topic} offset={r.Offset}"); }
}
public class KafkaLogConsumer : BackgroundService {
  private readonly ILogger<KafkaLogConsumer> _logger; public KafkaLogConsumer(ILogger<KafkaLogConsumer> logger)=>_logger=logger;
  protected override async Task ExecuteAsync(CancellationToken ct){ await Task.Delay(TimeSpan.FromSeconds(5), ct); var cfg=new ConsumerConfig{ BootstrapServers="kafka:29092", GroupId="sre-aggregator", AutoOffsetReset=AutoOffsetReset.Earliest, EnableAutoCommit=false }; using var c=new ConsumerBuilder<string,string>(cfg).Build(); try{ c.Subscribe("logs.raw"); }catch(Exception ex){ _logger.LogWarning(ex,"Subscribe failed"); return; } while(!ct.IsCancellationRequested){ try{ var r=c.Consume(ct); _logger.LogInformation("[Apache Kafka] logs.raw offset={Offset}", r.Offset); c.Commit(r);}catch(OperationCanceledException){ break; }catch(Exception ex){ _logger.LogWarning(ex,"Consume error"); await Task.Delay(2000, ct); } } c.Close(); }
}
public class IncidentService : IIncidentService {
  private readonly ILogStore _store; private readonly List<IncidentReport> _incidents = new();
  public IncidentService(ILogStore store){ _store=store; }
  public List<IncidentReport> All()=>_incidents;
  public IncidentReport? Latest(string service)=>_incidents.LastOrDefault(i=>i.Service==service);
  public Task<IncidentReport> DetectAsync(string service="checkout"){
    var logs = _store.All().Where(l=>l.Service==service).ToList();
    if(logs.Count<100){ var empty=new IncidentReport(service,120,120,0.2,0.2,"none","INFO","No anomaly - not enough data","Not enough logs yet.",DateTime.UtcNow); return Task.FromResult(empty); }
    // Split: first half = before, second half = after (simulates deploy in middle)
    var half = logs.Count/2;
    var before = logs.Take(half).ToList(); var after = logs.Skip(half).ToList();
    double p99Before = Percentile(before.Select(l=>l.LatencyMs).OrderBy(x=>x).ToList(), 0.99);
    double p99After = Percentile(after.Select(l=>l.LatencyMs).OrderBy(x=>x).ToList(), 0.99);
    double errBefore = before.Count(l=>l.StatusCode>=500)*100.0/Math.Max(1,before.Count);
    double errAfter = after.Count(l=>l.StatusCode>=500)*100.0/Math.Max(1,after.Count);
    bool anomaly = p99After>500 && errAfter>2;
    string severity = anomaly ? "CRITICAL" : "INFO";
    string rec = anomaly ? "ROLLBACK Deploy #482, failover to secondary, page on-call" : "No action - system healthy";
    string summary = anomaly ? $"CRITICAL: {service} p99 jumped {p99Before:F0}ms -> {p99After:F0}ms, error rate {errBefore:F1}% -> {errAfter:F1}%. Correlates with deploy." : $"Healthy: {service} p99 {p99After:F0}ms, error rate {errAfter:F1}%.";
    var deploy = after.LastOrDefault()?.DeployId ?? "none";
    var report = new IncidentReport(service, Math.Round(p99Before,0), Math.Round(p99After,0), Math.Round(errBefore,2), Math.Round(errAfter,2), deploy, severity, rec, summary, DateTime.UtcNow);
    _incidents.Add(report); return Task.FromResult(report);
  }
  private static double Percentile(List<int> sorted, double p){ if(sorted.Count==0) return 0; int idx=(int)Math.Ceiling(p*sorted.Count)-1; idx=Math.Clamp(idx,0,sorted.Count-1); return sorted[idx]; }
}
public class AnalystAgentService : IAnalystAgent {
  public Task<object> AnalyzeAsync(IncidentReport inc){
    string text = inc.Severity=="CRITICAL"
      ? $"{inc.Service} service p99 jumped from {inc.P99Before}ms to {inc.P99After}ms — 7x slower. Error rate went from {inc.ErrorRateBefore}% to {inc.ErrorRateAfter}%. This correlates with deploy {inc.DeployId}. This is the same pattern as cascading outages: one slow service drags down everything downstream. Recommendation: {inc.Recommendation}. Do NOT wait for more data — rollback now, then investigate root cause."
      : $"{inc.Service} is healthy. p99 is {inc.P99After}ms, error rate {inc.ErrorRateAfter}%. No rollback needed. Continue monitoring.";
    return Task.FromResult<object>(new { inc.Service, inc.Severity, inc.Summary, AnalystNarrative=text, inc.Recommendation, inc.DetectedAt });
  }
}
