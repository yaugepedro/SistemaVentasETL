using System.Diagnostics;
using SistemaVentasETL.Data.Interfaces;

namespace SistemaVentasETL.Load;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ISalesRepository _salesRepository;
    private readonly IProductApiRepository _productApiRepository;
    private readonly IHostApplicationLifetime _applicationLifetime;
    private readonly ITemporaryFileRepository _temporaryFileRepository;
    private readonly ICustomerCsvRepository _customerCsvRepository;

    public Worker(
        ILogger<Worker> logger,
        ISalesRepository salesRepository,
        IProductApiRepository productApiRepository,
        ICustomerCsvRepository customerCsvRepository,
        ITemporaryFileRepository temporaryFileRepository,
        IHostApplicationLifetime applicationLifetime)
    {
        _logger = logger;
        _salesRepository = salesRepository;
        _productApiRepository = productApiRepository;
        _customerCsvRepository = customerCsvRepository;
        _temporaryFileRepository = temporaryFileRepository;
        _applicationLifetime = applicationLifetime;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var totalStopwatch = Stopwatch.StartNew();

        try
        {
            _logger.LogInformation(
                "Iniciando las extracciones en paralelo.");

            await Task.WhenAll(
                ExtractSalesAsync(stoppingToken),
                ExtractProductsFromApiAsync(stoppingToken),
                ExtractCustomersFromCsvAsync(stoppingToken));

            totalStopwatch.Stop();

            _logger.LogInformation(
                "Todas las extracciones finalizaron correctamente.");

            _logger.LogInformation(
                "Tiempo total: {ElapsedMilliseconds} ms",
                totalStopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "El proceso de extracciÃ³n fue cancelado.");
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "OcurriÃ³ un error durante el proceso de extracciÃ³n.");
        }
        finally
        {
            _logger.LogInformation(
                "El proceso finalizarÃ¡ en 10 segundos.");

            await Task.Delay(
                TimeSpan.FromSeconds(10),
                CancellationToken.None);

            _applicationLifetime.StopApplication();
        }
    }

    private async Task ExtractSalesAsync(
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Iniciando la extracciÃ³n de ventas con ADO.NET.");

        var sales =
            await _salesRepository.GetSalesAsync(cancellationToken);

        await _temporaryFileRepository.SaveJsonAsync(
            "ventas-db.json",
            sales,
            cancellationToken);

        stopwatch.Stop();

        _logger.LogInformation(
            "Las ventas fueron guardadas en Temp/ventas-db.json.");

        _logger.LogInformation(
            "Ventas extraÃ­das: {RecordCount}",
            sales.Count);

        _logger.LogInformation(
            "Tiempo de extracciÃ³n de ventas: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }

    private async Task ExtractProductsFromApiAsync(
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Iniciando la extracciÃ³n de productos desde la API REST.");

        var products =
            await _productApiRepository.GetProductsAsync(
                cancellationToken);

        await _temporaryFileRepository.SaveJsonAsync(
            "productos-api.json",
            products,
            cancellationToken);

        stopwatch.Stop();

        _logger.LogInformation(
            "Los productos fueron guardados en Temp/productos-api.json.");

        _logger.LogInformation(
            "Productos extraÃ­dos desde la API: {RecordCount}",
            products.Count);

        _logger.LogInformation(
            "Tiempo de extracciÃ³n desde la API: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }

    private async Task ExtractCustomersFromCsvAsync(
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Iniciando la extracciÃ³n de clientes desde el archivo CSV.");

        var customers =
            await _customerCsvRepository.GetCustomersAsync(
                cancellationToken);

        await _temporaryFileRepository.SaveJsonAsync(
            "clientes-csv.json",
            customers,
            cancellationToken);

        stopwatch.Stop();

        _logger.LogInformation(
            "Los clientes fueron guardados en Temp/clientes-csv.json.");

        _logger.LogInformation(
            "Clientes extraÃ­dos desde CSV: {RecordCount}",
            customers.Count);

        _logger.LogInformation(
            "Tiempo de extracciÃ³n desde CSV: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }
}

