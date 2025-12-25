# Script d'Import du Flow NiFi
# Copie le fichier JSON dans le conteneur NiFi

Write-Host "`nImport du flow NiFi..." -ForegroundColor Cyan

# Copier le flow dans le conteneur
docker cp "scripts/airlineflow (1).json" nifi:/tmp/airlineflow.json

if ($LASTEXITCODE -eq 0) {
    Write-Host "✓ Flow copié dans le conteneur NiFi" -ForegroundColor Green
    Write-Host ""
    Write-Host "Maintenant, suivez ces étapes dans l'interface NiFi :" -ForegroundColor Yellow
    Write-Host "1. Accéder à http://localhost:8080/nifi"
    Write-Host "2. Login : admin / adminadminadmin"
    Write-Host "3. Clic droit sur le canvas → Upload Process Group"
    Write-Host "4. Sélectionner : /tmp/airlineflow.json"
    Write-Host "5. Cliquer sur 'Upload'"
    Write-Host ""
} else {
    Write-Host "✗ Erreur lors de la copie" -ForegroundColor Red
}
