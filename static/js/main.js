console.log("Mayacorp System Carregado com Sucesso!");

// Exemplo: Se quiser fechar alertas automaticamente depois
setTimeout(function() {
    let alerts = document.querySelectorAll('.alert');
    alerts.forEach(alert => {
        alert.style.display = 'none';
    });
}, 5000);