namespace SreLab.Api;
public record GenerateLogsRequest(string Service, int Count, bool InjectIncident, string DeployId);
public record LogEvent(string Service, int LatencyMs, int StatusCode, string DeployId, DateTime Timestamp);
public record IncidentReport(string Service, double P99Before, double P99After, double ErrorRateBefore, double ErrorRateAfter, string DeployId, string Severity, string Recommendation, string Summary, DateTime DetectedAt);
public interface ILogStore { Task<int> GenerateAsync(string service, int count, bool incident, string deployId, IKafkaProducer kafka); List<LogEvent> All(); }
public class InMemoryLogStore : ILogStore {
  private readonly List<LogEvent> _logs = new(); private readonly Random _rnd = new();
  public List<LogEvent> All() => _logs;
  public async Task<int> GenerateAsync(string service, int count, bool incident, string deployId, IKafkaProducer kafka){
    for(int i=0;i<count;i++){
      int latency; int status;
      if(incident){ latency = 600 + _rnd.Next(0,600); status = _rnd.NextDouble()<0.04 ? 500 : 200; }
      else { latency = 80 + _rnd.Next(0,80); status = _rnd.NextDouble()<0.002 ? 500 : 200; }
      var ev = new LogEvent(service, latency, status, deployId, DateTime.UtcNow);
      _logs.Add(ev); await kafka.PublishAsync("logs.raw", ev);
    }
    return count;
  }
}
public interface IKafkaProducer { Task PublishAsync(string topic, object payload); }
public interface IIncidentService { Task<IncidentReport> DetectAsync(string service="checkout"); List<IncidentReport> All(); IncidentReport? Latest(string service); }
public interface IAnalystAgent { Task<object> AnalyzeAsync(IncidentReport inc); }
