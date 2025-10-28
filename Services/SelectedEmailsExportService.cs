using MailArchiver.Data;
using MailArchiver.Models;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Collections.Concurrent;
using System.IO.Compression;
using System.Text;
using MimeKit;
using System.Globalization;
using QuestPDF.Fluent;
using QuestPDF.Infrastructure;
using System.Text.RegularExpressions;
using System.Net;

namespace MailArchiver.Services
{
    public class SelectedEmailsExportService : BackgroundService, ISelectedEmailsExportService
    {
        private readonly IServiceProvider _serviceProvider;
        private readonly ILogger<SelectedEmailsExportService> _logger;
        private readonly BatchOperationOptions _batchOptions;
        private readonly ConcurrentQueue<SelectedEmailsExportJob> _jobQueue = new();
        private readonly ConcurrentDictionary<string, SelectedEmailsExportJob> _allJobs = new();
        private readonly Timer _cleanupTimer;
        private CancellationTokenSource? _currentJobCancellation;
        private readonly string _exportsPath;
        private readonly TimeZoneOptions _timeZoneOptions;

        public SelectedEmailsExportService(
            IServiceProvider serviceProvider, 
            ILogger<SelectedEmailsExportService> logger, 
            IWebHostEnvironment environment, 
            IOptions<BatchOperationOptions> batchOptions,
            IOptions<TimeZoneOptions> timeZoneOptions)
        {
            _serviceProvider = serviceProvider;
            _logger = logger;
            _batchOptions = batchOptions.Value;
            _timeZoneOptions = timeZoneOptions.Value;
            _exportsPath = Path.Combine(environment.ContentRootPath, "exports");

            QuestPDF.Settings.License = LicenseType.Community;

            // Create exports directory if it doesn't exist
            Directory.CreateDirectory(_exportsPath);

            // Cleanup timer: Remove old jobs and files every day
            _cleanupTimer = new Timer(
                callback: _ => CleanupOldJobs(),
                state: null,
                dueTime: TimeSpan.FromHours(24),
                period: TimeSpan.FromHours(24)
            );
        }

        public string QueueExport(List<int> emailIds, AccountExportFormat format, string userId)
        {
            var job = new SelectedEmailsExportJob
            {
                EmailIds = emailIds,
                Format = format,
                UserId = userId,
                Status = SelectedEmailsExportJobStatus.Queued,
                TotalEmails = emailIds.Count
            };

            _allJobs[job.JobId] = job;
            _jobQueue.Enqueue(job);
            _logger.LogInformation("Queued export job {JobId} for {Count} selected emails in {Format} format",
                job.JobId, emailIds.Count, job.Format);
            return job.JobId;
        }

        public SelectedEmailsExportJob? GetJob(string jobId)
        {
            return _allJobs.TryGetValue(jobId, out var job) ? job : null;
        }

        public List<SelectedEmailsExportJob> GetActiveJobs()
        {
            // Return all jobs from the last 24 hours, prioritizing active jobs
            var cutoff = DateTime.UtcNow.AddHours(-24);
            return _allJobs.Values
                .Where(j => j.Created >= cutoff)
                .OrderByDescending(j => j.Status == SelectedEmailsExportJobStatus.Queued || j.Status == SelectedEmailsExportJobStatus.Running)
                .ThenByDescending(j => j.Created)
                .ToList();
        }
        
        public List<SelectedEmailsExportJob> GetAllJobs()
        {
            return _allJobs.Values
                .OrderByDescending(j => j.Status == SelectedEmailsExportJobStatus.Queued || j.Status == SelectedEmailsExportJobStatus.Running)
                .ThenByDescending(j => j.Created)
                .ToList();
        }

        public bool CancelJob(string jobId)
        {
            if (_allJobs.TryGetValue(jobId, out var job))
            {
                if (job.Status == SelectedEmailsExportJobStatus.Queued)
                {
                    job.Status = SelectedEmailsExportJobStatus.Cancelled;
                    job.Completed = DateTime.UtcNow;
                    _logger.LogInformation("Cancelled queued export job {JobId}", jobId);
                    return true;
                }
                else if (job.Status == SelectedEmailsExportJobStatus.Running)
                {
                    job.Status = SelectedEmailsExportJobStatus.Cancelled;
                    _currentJobCancellation?.Cancel();
                    _logger.LogInformation("Requested cancellation of running export job {JobId}", jobId);
                    return true;
                }
            }
            return false;
        }

        public FileResult? GetExportForDownload(string jobId)
        {
            var job = GetJob(jobId);
            if (job == null || job.Status != SelectedEmailsExportJobStatus.Completed || string.IsNullOrEmpty(job.OutputFilePath))
            {
                return null;
            }

            if (!File.Exists(job.OutputFilePath))
            {
                return null;
            }

            var fileName = $"selected_emails_export_{job.Created:yyyyMMdd_HHmmss}.zip";

            return new FileResult
            {
                FilePath = job.OutputFilePath,
                FileName = fileName,
                ContentType = "application/zip"
            };
        }

        public bool MarkAsDownloaded(string jobId)
        {
            if (_allJobs.TryGetValue(jobId, out var job))
            {
                job.Status = SelectedEmailsExportJobStatus.Downloaded;
                job.Completed = DateTime.UtcNow; // Update completion time for cleanup
                
                // Don't delete the file immediately - it's still being streamed to the client
                // The file will be cleaned up automatically by the cleanup job after 7 days
                _logger.LogInformation("Marked export job {JobId} as downloaded. File will be cleaned up automatically.", jobId);
                
                return true;
            }
            return false;
        }

        public void CleanupOldJobs()
        {
            var cutoffTime = DateTime.UtcNow.AddDays(-7); // Remove jobs older than 7 days
            var toRemove = _allJobs.Values
                .Where(j => j.Completed.HasValue && j.Completed < cutoffTime)
                .ToList();

            foreach (var job in toRemove)
            {
                _allJobs.TryRemove(job.JobId, out _);

                // Delete associated file
                if (!string.IsNullOrEmpty(job.OutputFilePath))
                {
                    try
                    {
                        if (File.Exists(job.OutputFilePath))
                        {
                            File.Delete(job.OutputFilePath);
                            _logger.LogInformation("Deleted old export file {FilePath}", job.OutputFilePath);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete old export file {FilePath}", job.OutputFilePath);
                    }
                }

                if (!string.IsNullOrEmpty(job.OutputDirectoryPath))
                {
                    try
                    {
                        if (Directory.Exists(job.OutputDirectoryPath))
                        {
                            Directory.Delete(job.OutputDirectoryPath, recursive: true);
                            _logger.LogInformation("Deleted old export directory {Directory}", job.OutputDirectoryPath);
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete old export directory {Directory}", job.OutputDirectoryPath);
                    }
                }
            }

            if (toRemove.Any())
            {
                _logger.LogInformation("Cleaned up {Count} old export jobs", toRemove.Count);
            }
        }

        public override Task StartAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Selected Emails Export Background Service is starting.");
            return base.StartAsync(cancellationToken);
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _logger.LogInformation("Selected Emails Export Background Service started");

            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (_jobQueue.TryDequeue(out var job))
                    {
                        if (job.Status == SelectedEmailsExportJobStatus.Cancelled)
                        {
                            _logger.LogInformation("Skipping cancelled export job {JobId}", job.JobId);
                            continue;
                        }

                        await ProcessJob(job, stoppingToken);
                    }
                    else
                    {
                        await Task.Delay(100, stoppingToken);
                    }
                }
                catch (OperationCanceledException)
                {
                    _logger.LogInformation("Selected Emails Export Background Service stopping");
                    break;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Error in Selected Emails Export Background Service");
                    await Task.Delay(1000, stoppingToken);
                }
            }
        }

        public override Task StopAsync(CancellationToken cancellationToken)
        {
            _logger.LogInformation("Selected Emails Export Background Service is stopping.");
            return base.StopAsync(cancellationToken);
        }

        private async Task ProcessJob(SelectedEmailsExportJob job, CancellationToken stoppingToken)
        {
            _currentJobCancellation = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
            var cancellationToken = _currentJobCancellation.Token;

            try
            {
                job.Status = SelectedEmailsExportJobStatus.Running;
                job.Started = DateTime.UtcNow;

                _logger.LogInformation("Starting export job {JobId} for {Count} selected emails in {Format} format",
                    job.JobId, job.EmailIds.Count, job.Format);

                using var scope = _serviceProvider.CreateScope();
                var context = scope.ServiceProvider.GetRequiredService<MailArchiverDbContext>();

                // Generate output file path
                var fileName = $"export_selected_{job.JobId}_{DateTime.UtcNow:yyyyMMddHHmmss}.zip";
                job.OutputFilePath = Path.Combine(_exportsPath, fileName);

                // Perform export based on format
                if (job.Format == AccountExportFormat.EML)
                {
                    await ExportToEmlFormat(job, context, cancellationToken);
                }
                else if (job.Format == AccountExportFormat.MBox)
                {
                    await ExportToMBoxFormat(job, context, cancellationToken);
                }
                else if (job.Format == AccountExportFormat.Pdf)
                {
                    await ExportToPdfFormat(job, context, cancellationToken);
                }

                if (job.Status != SelectedEmailsExportJobStatus.Cancelled)
                {
                    job.Status = SelectedEmailsExportJobStatus.Completed;
                    job.Completed = DateTime.UtcNow;
                    
                    // Get file size
                    if (File.Exists(job.OutputFilePath))
                    {
                        job.OutputFileSize = new FileInfo(job.OutputFilePath).Length;
                    }

                    _logger.LogInformation("Completed export job {JobId}. Processed: {Processed} emails, File size: {Size} bytes",
                        job.JobId, job.ProcessedEmails, job.OutputFileSize);
                }
            }
            catch (OperationCanceledException)
            {
                job.Status = SelectedEmailsExportJobStatus.Cancelled;
                job.Completed = DateTime.UtcNow;
                
                // Delete partial export file
                if (!string.IsNullOrEmpty(job.OutputFilePath) && File.Exists(job.OutputFilePath))
                {
                    try
                    {
                        File.Delete(job.OutputFilePath);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete partial export file {FilePath}", job.OutputFilePath);
                    }
                }

                if (!string.IsNullOrEmpty(job.OutputDirectoryPath) && Directory.Exists(job.OutputDirectoryPath))
                {
                    try
                    {
                        Directory.Delete(job.OutputDirectoryPath, recursive: true);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, "Failed to delete partial export directory {Directory}", job.OutputDirectoryPath);
                    }
                }

                _logger.LogInformation("Export job {JobId} was cancelled", job.JobId);
            }
            catch (Exception ex)
            {
                job.Status = SelectedEmailsExportJobStatus.Failed;
                job.Completed = DateTime.UtcNow;
                job.ErrorMessage = ex.Message;
                _logger.LogError(ex, "Export job {JobId} failed", job.JobId);

                if (!string.IsNullOrEmpty(job.OutputDirectoryPath) && Directory.Exists(job.OutputDirectoryPath))
                {
                    try
                    {
                        Directory.Delete(job.OutputDirectoryPath, recursive: true);
                    }
                    catch (Exception cleanupEx)
                    {
                        _logger.LogWarning(cleanupEx, "Failed to clean export directory after failure {Directory}", job.OutputDirectoryPath);
                    }
                }
            }
            finally
            {
                _currentJobCancellation?.Dispose();
                _currentJobCancellation = null;
            }
        }

        private async Task ExportToEmlFormat(SelectedEmailsExportJob job, MailArchiverDbContext context, CancellationToken cancellationToken)
        {
            using var zipStream = new FileStream(job.OutputFilePath, FileMode.Create, FileAccess.Write);
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Create);

            // Get distinct folder names for selected emails
            var folderNames = await context.ArchivedEmails
                .Where(e => job.EmailIds.Contains(e.Id))
                .Select(e => e.FolderName)
                .Distinct()
                .ToListAsync(cancellationToken);

            // Process emails for each folder
            foreach (var folderName in folderNames)
            {
                await ProcessEmailsForEmlExportByFolder(job, context, archive, folderName, cancellationToken);
            }
        }

        private async Task ProcessEmailsForEmlExportByFolder(SelectedEmailsExportJob job, MailArchiverDbContext context, ZipArchive archive, string folderName, CancellationToken cancellationToken)
        {
            var emails = context.ArchivedEmails
                .Where(e => job.EmailIds.Contains(e.Id) && e.FolderName == folderName)
                .Include(e => e.Attachments)
                .AsAsyncEnumerable();

            var emailIndex = 1;
            await foreach (var email in emails.WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    job.CurrentEmailSubject = email.Subject;

                    // Create MIME message from archived email
                    var mimeMessage = await CreateMimeMessageFromArchived(email);

                    // Generate safe filename with In/Out indicator
                    var safeSubject = SanitizeFileName(email.Subject);
                    var inOutIndicator = email.IsOutgoing ? "Out" : "In";
                    var fileName = $"{emailIndex:D6}_{email.SentDate:yyyyMMdd_HHmmss}_{inOutIndicator}_{safeSubject}.eml";
                    var entryName = $"{SanitizeFileName(folderName)}/{fileName}";

                    // Add to ZIP archive
                    var entry = archive.CreateEntry(entryName);
                    using var entryStream = entry.Open();
                    await mimeMessage.WriteToAsync(entryStream, cancellationToken);

                    job.ProcessedEmails++;
                    emailIndex++;

                    // Small pause every 10 emails
                    if (job.ProcessedEmails % 10 == 0 && _batchOptions.PauseBetweenEmailsMs > 0)
                    {
                        await Task.Delay(_batchOptions.PauseBetweenEmailsMs, cancellationToken);
                    }

                    // Log progress every 100 emails
                    if (job.ProcessedEmails % 100 == 0)
                    {
                        var progressPercent = job.TotalEmails > 0 ? (job.ProcessedEmails * 100.0 / job.TotalEmails) : 0;
                        _logger.LogInformation("Job {JobId}: Processed {Processed} emails ({Progress:F1}%)",
                            job.JobId, job.ProcessedEmails, progressPercent);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Job {JobId}: Failed to export email {EmailId}: {Subject}", 
                        job.JobId, email.Id, email.Subject);
                }
            }
        }

        private async Task ExportToMBoxFormat(SelectedEmailsExportJob job, MailArchiverDbContext context, CancellationToken cancellationToken)
        {
            using var zipStream = new FileStream(job.OutputFilePath, FileMode.Create, FileAccess.Write);
            using var archive = new ZipArchive(zipStream, ZipArchiveMode.Create);

            // Get distinct folder names for selected emails
            var folderNames = await context.ArchivedEmails
                .Where(e => job.EmailIds.Contains(e.Id))
                .Select(e => e.FolderName)
                .Distinct()
                .ToListAsync(cancellationToken);

            // Process emails for each folder
            foreach (var folderName in folderNames)
            {
                var mboxEntry = archive.CreateEntry($"{SanitizeFileName(folderName)}.mbox");
                using var mboxStream = mboxEntry.Open();
                await ProcessEmailsForMBoxExportByFolder(job, context, mboxStream, folderName, cancellationToken);
            }
        }

        private async Task ProcessEmailsForMBoxExportByFolder(SelectedEmailsExportJob job, MailArchiverDbContext context, Stream mboxStream, string folderName, CancellationToken cancellationToken)
        {
            using var writer = new StreamWriter(mboxStream, Encoding.UTF8, leaveOpen: true);

            var emails = context.ArchivedEmails
                .Where(e => job.EmailIds.Contains(e.Id) && e.FolderName == folderName)
                .Include(e => e.Attachments)
                .OrderBy(e => e.SentDate)
                .AsAsyncEnumerable();

            await foreach (var email in emails.WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                try
                {
                    job.CurrentEmailSubject = email.Subject;

                    // Write mbox separator line
                    var fromLine = CreateMBoxFromLine(email);
                    await writer.WriteLineAsync(fromLine);

                    // Create MIME message and write to mbox
                    var mimeMessage = await CreateMimeMessageFromArchived(email);
                    
                    using var messageStream = new MemoryStream();
                    await mimeMessage.WriteToAsync(messageStream, cancellationToken);
                    messageStream.Position = 0;

                    using var messageReader = new StreamReader(messageStream, Encoding.UTF8);
                    string? line;
                    while ((line = await messageReader.ReadLineAsync()) != null)
                    {
                        // Escape lines that start with "From " (mbox format requirement)
                        if (line.StartsWith("From "))
                        {
                            line = ">" + line;
                        }
                        await writer.WriteLineAsync(line);
                    }

                    // Add empty line after message
                    await writer.WriteLineAsync();

                    job.ProcessedEmails++;

                    // Small pause every 10 emails
                    if (job.ProcessedEmails % 10 == 0 && _batchOptions.PauseBetweenEmailsMs > 0)
                    {
                        await Task.Delay(_batchOptions.PauseBetweenEmailsMs, cancellationToken);
                    }

                    // Log progress every 100 emails
                    if (job.ProcessedEmails % 100 == 0)
                    {
                        var progressPercent = job.TotalEmails > 0 ? (job.ProcessedEmails * 100.0 / job.TotalEmails) : 0;
                        _logger.LogInformation("Job {JobId}: Processed {Processed} emails ({Progress:F1}%)",
                            job.JobId, job.ProcessedEmails, progressPercent);
                    }
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Job {JobId}: Failed to export email {EmailId}: {Subject}", 
                        job.JobId, email.Id, email.Subject);
                }
            }

            await writer.FlushAsync();
        }

        private async Task ExportToPdfFormat(SelectedEmailsExportJob job, MailArchiverDbContext context, CancellationToken cancellationToken)
        {
            var exportFolderName = Path.GetFileNameWithoutExtension(job.OutputFilePath) + "_pdf";
            var exportFolderPath = Path.Combine(_exportsPath, exportFolderName);

            if (Directory.Exists(exportFolderPath))
            {
                Directory.Delete(exportFolderPath, recursive: true);
            }

            Directory.CreateDirectory(exportFolderPath);
            job.OutputDirectoryPath = exportFolderPath;

            var threadDirectories = new Dictionary<string, ThreadExportInfo>(StringComparer.OrdinalIgnoreCase);

            var emails = context.ArchivedEmails
                .Where(e => job.EmailIds.Contains(e.Id))
                .Include(e => e.Attachments)
                .OrderBy(e => e.SentDate)
                .AsAsyncEnumerable();

            await foreach (var email in emails.WithCancellation(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                job.CurrentEmailSubject = email.Subject;

                var conversationKey = NormalizeSubject(email.Subject);
                if (string.IsNullOrWhiteSpace(conversationKey))
                {
                    conversationKey = "No Subject";
                }

                if (!threadDirectories.TryGetValue(conversationKey, out var threadInfo))
                {
                    var baseDirectoryName = SanitizeFileName(conversationKey);
                    var uniqueDirectoryName = EnsureUniqueDirectoryName(exportFolderPath, baseDirectoryName);
                    var directoryPath = Path.Combine(exportFolderPath, uniqueDirectoryName);
                    Directory.CreateDirectory(directoryPath);
                    threadInfo = new ThreadExportInfo(directoryPath);
                    threadDirectories[conversationKey] = threadInfo;
                }

                threadInfo.EmailCounter++;

                var sanitizedSubject = SanitizeFileName(email.Subject);
                var prefix = $"{threadInfo.EmailCounter:D4}_{email.SentDate:yyyyMMdd_HHmmss}_{(email.IsOutgoing ? "Out" : "In")}";
                var pdfFileName = $"{prefix}_{sanitizedSubject}.pdf";
                pdfFileName = EnsureUniqueFileName(threadInfo.DirectoryPath, pdfFileName);
                var pdfFilePath = Path.Combine(threadInfo.DirectoryPath, pdfFileName);

                await GeneratePdfForEmail(email, pdfFilePath, cancellationToken);

                job.ProcessedEmails++;

                if (job.ProcessedEmails % 10 == 0 && _batchOptions.PauseBetweenEmailsMs > 0)
                {
                    await Task.Delay(_batchOptions.PauseBetweenEmailsMs, cancellationToken);
                }

                if (job.ProcessedEmails % 100 == 0)
                {
                    var progressPercent = job.TotalEmails > 0 ? (job.ProcessedEmails * 100.0 / job.TotalEmails) : 0;
                    _logger.LogInformation("Job {JobId}: Processed {Processed} selected emails ({Progress:F1}%)",
                        job.JobId, job.ProcessedEmails, progressPercent);
                }
            }

            if (!string.IsNullOrEmpty(job.OutputFilePath))
            {
                if (File.Exists(job.OutputFilePath))
                {
                    File.Delete(job.OutputFilePath);
                }

                ZipFile.CreateFromDirectory(exportFolderPath, job.OutputFilePath, CompressionLevel.Optimal, includeBaseDirectory: false);
            }
        }

        private async Task GeneratePdfForEmail(ArchivedEmail email, string filePath, CancellationToken cancellationToken)
        {
            var bodyContent = string.IsNullOrWhiteSpace(email.Body)
                ? ConvertHtmlToPlainText(email.HtmlBody)
                : email.Body;

            var attachmentDescriptions = email.Attachments?
                .Select(a =>
                {
                    var fileName = Path.GetFileName(a.FileName);
                    if (string.IsNullOrWhiteSpace(fileName))
                    {
                        fileName = "attachment";
                    }

                    var size = a.Size;
                    var contentType = string.IsNullOrWhiteSpace(a.ContentType) ? string.Empty : a.ContentType;
                    if (string.IsNullOrEmpty(contentType))
                    {
                        return string.IsNullOrEmpty(fileName)
                            ? null
                            : $"{fileName} ({FormatFileSize(size)})";
                    }

                    return $"{fileName} ({contentType}, {FormatFileSize(size)})";
                })
                .Where(a => !string.IsNullOrWhiteSpace(a))
                .ToList() ?? new List<string>();

            var sentDate = email.SentDate.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            var receivedDate = email.ReceivedDate.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

            await Task.Run(() =>
            {
                var document = Document.Create(container =>
                {
                    container.Page(page =>
                    {
                        page.Size(PageSizes.A4);
                        page.Margin(35);
                        page.DefaultTextStyle(x => x.FontSize(11));

                        page.Content().Column(column =>
                        {
                            column.Spacing(10);

                            column.Item().Text(string.IsNullOrWhiteSpace(email.Subject) ? "(No Subject)" : email.Subject)
                                .FontSize(18)
                                .SemiBold();

                            column.Item().Text(text =>
                            {
                                text.Line($"Sent: {sentDate}");
                                text.Line($"Received: {receivedDate}");
                            });

                            column.Item().Text(text =>
                            {
                                text.Line($"From: {FormatAddressList(email.From)}");
                                if (!string.IsNullOrWhiteSpace(email.To))
                                {
                                    text.Line($"To: {FormatAddressList(email.To)}");
                                }
                                if (!string.IsNullOrWhiteSpace(email.Cc))
                                {
                                    text.Line($"Cc: {FormatAddressList(email.Cc)}");
                                }
                                if (!string.IsNullOrWhiteSpace(email.Bcc))
                                {
                                    text.Line($"Bcc: {FormatAddressList(email.Bcc)}");
                                }
                            });

                            if (attachmentDescriptions.Any())
                            {
                                column.Item().Column(list =>
                                {
                                    list.Spacing(2);
                                    list.Item().Text("Attachments:").SemiBold();
                                    foreach (var attachment in attachmentDescriptions)
                                    {
                                        list.Item().Text($"• {attachment}");
                                    }
                                });
                            }

                            if (!string.IsNullOrWhiteSpace(bodyContent))
                            {
                                column.Item().Column(body =>
                                {
                                    body.Item().Text("Body:").SemiBold();
                                    body.Item().PaddingTop(4).Text(bodyContent);
                                });
                            }
                            else
                            {
                                column.Item().Text("Body: (empty)").Italic();
                            }
                        });
                    });
                });

                document.GeneratePdf(filePath);
            }, cancellationToken);
        }

        private string NormalizeSubject(string? subject)
        {
            if (string.IsNullOrWhiteSpace(subject))
            {
                return string.Empty;
            }

            var normalized = subject.Trim();
            normalized = Regex.Replace(normalized, @"^(re|fw|fwd):", string.Empty, RegexOptions.IgnoreCase).Trim();
            return normalized;
        }

        private string EnsureUniqueDirectoryName(string rootPath, string baseName)
        {
            if (string.IsNullOrWhiteSpace(baseName))
            {
                baseName = "Conversation";
            }

            var candidate = baseName;
            var index = 1;

            while (Directory.Exists(Path.Combine(rootPath, candidate)))
            {
                candidate = $"{baseName}_{index}";
                index++;
            }

            return candidate;
        }

        private string EnsureUniqueFileName(string directoryPath, string fileName)
        {
            var baseName = Path.GetFileNameWithoutExtension(fileName);
            var extension = Path.GetExtension(fileName);

            var candidate = fileName;
            var index = 1;

            while (File.Exists(Path.Combine(directoryPath, candidate)))
            {
                candidate = $"{baseName}_{index}{extension}";
                index++;
            }

            return candidate;
        }

        private string ConvertHtmlToPlainText(string? html)
        {
            if (string.IsNullOrWhiteSpace(html))
            {
                return string.Empty;
            }

            var text = Regex.Replace(html, @"<br\s*/?>", "\n", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, @"</p>", "\n", RegexOptions.IgnoreCase);
            text = Regex.Replace(text, @"<p[^>]*>", string.Empty, RegexOptions.IgnoreCase);
            text = Regex.Replace(text, @"<[^>]+>", string.Empty);
            text = WebUtility.HtmlDecode(text);
            return text.Replace("\r", string.Empty).Trim();
        }

        private string FormatAddressList(string? addresses)
        {
            if (string.IsNullOrWhiteSpace(addresses))
            {
                return "-";
            }

            return string.Join(", ", addresses
                .Split(new[] { ';', ',' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(a => a.Trim())
                .Where(a => !string.IsNullOrWhiteSpace(a)));
        }

        private string FormatFileSize(long bytes)
        {
            if (bytes <= 0)
            {
                return "0 B";
            }

            string[] sizes = { "B", "KB", "MB", "GB" };
            var order = 0;
            double len = bytes;

            while (len >= 1024 && order < sizes.Length - 1)
            {
                order++;
                len /= 1024;
            }

            return $"{len:0.##} {sizes[order]}";
        }

        private class ThreadExportInfo
        {
            public ThreadExportInfo(string directoryPath)
            {
                DirectoryPath = directoryPath;
            }

            public string DirectoryPath { get; }
            public int EmailCounter { get; set; }
        }

        private async Task<MimeMessage> CreateMimeMessageFromArchived(ArchivedEmail email)
        {
            var message = new MimeMessage();

            // Set headers
            message.MessageId = email.MessageId;
            message.Subject = email.Subject;
            
            // The SentDate in the database is already in the configured display timezone (converted during sync)
            // We need to specify this timezone when creating the DateTimeOffset to preserve the correct time
            var displayTimeZone = TimeZoneInfo.FindSystemTimeZoneById(_timeZoneOptions.DisplayTimeZoneId);
            message.Date = new DateTimeOffset(email.SentDate, displayTimeZone.GetUtcOffset(email.SentDate));

            // Parse addresses
            if (!string.IsNullOrEmpty(email.From))
            {
                try
                {
                    message.From.AddRange(InternetAddressList.Parse(email.From));
                }
                catch
                {
                    message.From.Add(new MailboxAddress("", email.From));
                }
            }

            if (!string.IsNullOrEmpty(email.To))
            {
                try
                {
                    message.To.AddRange(InternetAddressList.Parse(email.To));
                }
                catch
                {
                    message.To.Add(new MailboxAddress("", email.To));
                }
            }

            if (!string.IsNullOrEmpty(email.Cc))
            {
                try
                {
                    message.Cc.AddRange(InternetAddressList.Parse(email.Cc));
                }
                catch
                {
                    message.Cc.Add(new MailboxAddress("", email.Cc));
                }
            }

            if (!string.IsNullOrEmpty(email.Bcc))
            {
                try
                {
                    message.Bcc.AddRange(InternetAddressList.Parse(email.Bcc));
                }
                catch
                {
                    message.Bcc.Add(new MailboxAddress("", email.Bcc));
                }
            }

            // Create body
            var bodyBuilder = new BodyBuilder();

            if (!string.IsNullOrEmpty(email.Body))
            {
                bodyBuilder.TextBody = email.Body;
            }

            if (!string.IsNullOrEmpty(email.HtmlBody))
            {
                bodyBuilder.HtmlBody = email.HtmlBody;
            }

            // Add attachments
            if (email.Attachments?.Any() == true)
            {
                // Separate inline attachments from regular attachments
                var inlineAttachments = email.Attachments.Where(a => !string.IsNullOrEmpty(a.ContentId)).ToList();
                var regularAttachments = email.Attachments.Where(a => string.IsNullOrEmpty(a.ContentId)).ToList();
                
                // Add inline attachments first so they can be referenced in the HTML body
                foreach (var attachment in inlineAttachments)
                {
                    try
                    {
                        var contentType = ContentType.Parse(attachment.ContentType);
                        var mimePart = bodyBuilder.LinkedResources.Add(attachment.FileName, attachment.Content, contentType);
                        mimePart.ContentId = attachment.ContentId;
                    }
                    catch
                    {
                        // Fallback for invalid content types
                        var mimePart = bodyBuilder.LinkedResources.Add(attachment.FileName, attachment.Content);
                        mimePart.ContentId = attachment.ContentId;
                    }
                }
                
                // Add regular attachments
                foreach (var attachment in regularAttachments)
                {
                    try
                    {
                        var contentType = ContentType.Parse(attachment.ContentType);
                        bodyBuilder.Attachments.Add(attachment.FileName, attachment.Content, contentType);
                    }
                    catch
                    {
                        // Fallback for invalid content types
                        bodyBuilder.Attachments.Add(attachment.FileName, attachment.Content);
                    }
                }
            }

            message.Body = bodyBuilder.ToMessageBody();
            return message;
        }

        private string CreateMBoxFromLine(ArchivedEmail email)
        {
            var fromAddress = "unknown@example.com";
            if (!string.IsNullOrEmpty(email.From))
            {
                try
                {
                    var addresses = InternetAddressList.Parse(email.From);
                    if (addresses.Mailboxes.Any())
                    {
                        fromAddress = addresses.Mailboxes.First().Address;
                    }
                }
                catch
                {
                    fromAddress = email.From.Contains("@") ? email.From : "unknown@example.com";
                }
            }

            // The SentDate in the database is already in the configured display timezone (converted during sync)
            // Format: "From address@domain.com Mon Jan 01 12:00:00 2024"
            var dateString = email.SentDate.ToString("ddd MMM dd HH:mm:ss yyyy", CultureInfo.InvariantCulture);
            return $"From {fromAddress} {dateString}";
        }

        private string SanitizeFileName(string fileName)
        {
            if (string.IsNullOrEmpty(fileName))
                return "untitled";

            // Remove invalid characters
            var invalidChars = Path.GetInvalidFileNameChars();
            var sanitized = new StringBuilder();

            foreach (var c in fileName)
            {
                if (invalidChars.Contains(c))
                {
                    sanitized.Append('_');
                }
                else
                {
                    sanitized.Append(c);
                }
            }

            var result = sanitized.ToString().Trim();
            if (result.Length > 50)
            {
                result = result.Substring(0, 50);
            }

            return string.IsNullOrEmpty(result) ? "untitled" : result;
        }

        public override void Dispose()
        {
            _cleanupTimer?.Dispose();
            _currentJobCancellation?.Dispose();
            base.Dispose();
        }
    }
}
