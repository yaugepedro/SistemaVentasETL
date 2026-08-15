using System.Diagnostics;
using SistemaVentasETL.Data.Interfaces;
using SistemaVentasETL.Load.Services.Interfaces;

namespace SistemaVentasETL.Load;

public sealed class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly ISalesRepository _salesRepository;
    private readonly IProductApiRepository _productApiRepository;
    private readonly ICustomerCsvRepository _customerCsvRepository;
    private readonly ITemporaryFileRepository _temporaryFileRepository;
    private readonly IDimensionLoadService _dimensionLoadService;
    private readonly IFactLoadService _factLoadService;
    private readonly IHostApplicationLifetime _applicationLifetime;

    public Worker(
        ILogger<Worker> logger,
        ISalesRepository salesRepository,
        IProductApiRepository productApiRepository,
        ICustomerCsvRepository customerCsvRepository,
        ITemporaryFileRepository temporaryFileRepository,
        IDimensionLoadService dimensionLoadService,
        IFactLoadService factLoadService,
        IHostApplicationLifetime applicationLifetime)
    {
        _logger = logger;
        _salesRepository = salesRepository;
        _productApiRepository = productApiRepository;
        _customerCsvRepository = customerCsvRepository;
        _temporaryFileRepository = temporaryFileRepository;
        _dimensionLoadService = dimensionLoadService;
        _factLoadService = factLoadService;
        _applicationLifetime = applicationLifetime;
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var totalStopwatch = Stopwatch.StartNew();

        try
        {
            _logger.LogInformation(
                "Iniciando el proceso ETL completo.");

            _logger.LogInformation(
                "Iniciando las extracciones en paralelo.");

            await Task.WhenAll(
                ExtractSalesAsync(stoppingToken),
                ExtractProductsFromApiAsync(stoppingToken),
                ExtractCustomersFromCsvAsync(stoppingToken));

            _logger.LogInformation(
                "Todas las extracciones finalizaron correctamente.");

            _logger.LogInformation(
                "Iniciando la carga de dimensiones en DW_Sistema_Ventas.");

            var loadResult =
                await _dimensionLoadService
                    .LoadDimensionsAsync(stoppingToken);

            if (!loadResult.IsSuccess)
            {
                throw new InvalidOperationException(
                    loadResult.Message);
            }

            _logger.LogInformation(
                "Carga de dimensiones completada correctamente.");

            _logger.LogInformation(
                "Registros cargados en las dimensiones: {RecordCount}",
                loadResult.Data);

            _logger.LogInformation(
                "Iniciando la carga de FactVentas en DW_Sistema_Ventas.");

            var factLoadResult =
                await _factLoadService.LoadFactVentasAsync(
                    stoppingToken);

            if (!factLoadResult.IsSuccess)
            {
                throw new InvalidOperationException(
                    factLoadResult.Message);
            }

            totalStopwatch.Stop();

            _logger.LogInformation(
                "Carga de FactVentas completada correctamente.");

            _logger.LogInformation(
                "Registros cargados en FactVentas: {RecordCount}",
                factLoadResult.Data);

            _logger.LogInformation(
                "Tiempo total del proceso ETL: {ElapsedMilliseconds} ms",
                totalStopwatch.ElapsedMilliseconds);
        }
        catch (OperationCanceledException)
            when (stoppingToken.IsCancellationRequested)
        {
            _logger.LogWarning(
                "El proceso ETL fue cancelado.");
        }
        catch (Exception exception)
        {
            _logger.LogError(
                exception,
                "Ocurrió un error durante el proceso ETL.");
        }
        finally
        {
            _logger.LogInformation(
                "El proceso finalizará en 10 segundos.");

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
            "Iniciando la extracción de ventas con ADO.NET.");

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
            "Ventas extraídas: {RecordCount}",
            sales.Count);

        _logger.LogInformation(
            "Tiempo de extracción de ventas: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }

    private async Task ExtractProductsFromApiAsync(
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Iniciando la extracción de productos desde la API REST.");

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
            "Productos extraídos desde la API: {RecordCount}",
            products.Count);

        _logger.LogInformation(
            "Tiempo de extracción desde la API: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }

    private async Task ExtractCustomersFromCsvAsync(
        CancellationToken cancellationToken)
    {
        var stopwatch = Stopwatch.StartNew();

        _logger.LogInformation(
            "Iniciando la extracción de clientes desde el archivo CSV.");

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
            "Clientes extraídos desde CSV: {RecordCount}",
            customers.Count);

        _logger.LogInformation(
            "Tiempo de extracción desde CSV: {ElapsedMilliseconds} ms",
            stopwatch.ElapsedMilliseconds);
    }
}