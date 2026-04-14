const API_BASE_URL = "https://apiexa618-playlist-retro-games.onrender.com";

let filtrosSelecionados = {
  genero: [],
  plataforma: [],
};

function adicionarFiltro(tipo) {
  const select = document.getElementById(`filtro-${tipo}`);
  const valor = select.value;

  if (valor && !filtrosSelecionados[tipo].includes(valor)) {
    filtrosSelecionados[tipo].push(valor);
    atualizarTelaDeTags(tipo);
  }
  select.value = "";
}

function removerFiltro(tipo, valor) {
  filtrosSelecionados[tipo] = filtrosSelecionados[tipo].filter(
    (item) => item !== valor,
  );
  atualizarTelaDeTags(tipo);
}

function atualizarTelaDeTags(tipo) {
  const container = document.getElementById(`tags-${tipo}`);
  container.innerHTML = filtrosSelecionados[tipo]
    .map(
      (valor) => `
        <div class="chip">
            ${valor}
            <span onclick="removerFiltro('${tipo}', '${valor}')">✕</span>
        </div>
    `,
    )
    .join("");
}

async function preencherListasDeFiltros() {
  try {
    const resGeneros = await fetch(`${API_BASE_URL}/categorias/generos`);
    const generos = await resGeneros.json();
    const selectGenero = document.getElementById("filtro-genero");
    generos.forEach((genero) => {
      selectGenero.innerHTML += `<option value="${genero}">${genero}</option>`;
    });

    const resPlataformas = await fetch(
      `${API_BASE_URL}/categorias/plataformas`,
    );
    const plataformas = await resPlataformas.json();
    const selectPlataforma = document.getElementById("filtro-plataforma");
    plataformas.forEach((plataforma) => {
      selectPlataforma.innerHTML += `<option value="${plataforma}">${plataforma}</option>`;
    });
  } catch (erro) {
    console.error("Erro ao carregar filtros:", erro);
  }
}

async function inicializarApp() {
  try {
    await preencherListasDeFiltros();
  } catch (erro) {
    console.error("API Offline ou Erro de conexão.");
    document.getElementById("grade-resultados").innerHTML =
      `<h3 style="color: var(--danger);">Servidor indisponível. Verifique o terminal Python.</h3>`;
  }
}

async function gerarPlaylist() {
  const anoInicio = document.getElementById("filtro-ano-inicio").value || 1990;
  const anoFim = document.getElementById("filtro-ano-fim").value || 2000;
  const nota = document.getElementById("filtro-nota").value || 0;
  const votosMinimos = document.getElementById("filtro-votos").value || 0;
  const tamanho = document.getElementById("filtro-tamanho").value;

  let url = new URL(`${API_BASE_URL}/jogos/playlist`);
  url.searchParams.append("ano_inicio", anoInicio);
  url.searchParams.append("ano_fim", anoFim);
  url.searchParams.append("nota_minima", nota);
  url.searchParams.append("votos_minimos", votosMinimos);

  filtrosSelecionados.genero.forEach((g) =>
    url.searchParams.append("genero", g),
  );
  filtrosSelecionados.plataforma.forEach((p) =>
    url.searchParams.append("plataforma", p),
  );

  const divResultados = document.getElementById("grade-resultados");
  const loading = document.getElementById("loading");

  divResultados.innerHTML = "";
  loading.style.display = "block";

  try {
    const resposta = await fetch(url);

    if (!resposta.ok) {
      const erroBackend = await resposta.json();
      throw new Error(erroBackend.detail);
    }

    const jogos = await resposta.json();
    loading.style.display = "none";

    if (jogos.length === 0) {
        throw new Error("Nenhum jogo encontrado com estes filtros.");
    }

    jogos.forEach((jogo) => {
      const imgSrc =
        jogo.imagem_url && jogo.imagem_url !== "Sem imagem"
          ? jogo.imagem_url
          : "https://via.placeholder.com/300x200/333333/aaaaaa?text=Sem+Capa";
      const card = `
                <div class="card">
                    <img src="${imgSrc}" alt="${jogo.titulo}">
                    <div class="card-info">
                        <h3 class="card-title">${jogo.titulo}</h3>
                        <div class="tag-container">
                            <span class="tag">Ano: ${jogo.ano}</span>
                            <span class="tag">Plataforma: ${jogo.plataforma || "N/A"}</span>
                            ${jogo.genero ? `<span class="tag">Gênero: ${jogo.genero}</span>` : ""}
                        </div>
                        <span class="nota">Nota: ${jogo.nota.toFixed(1)} (${jogo.votos} votos)</span>
                        <div class="botoes-acao">
                            <a href="${jogo.link_download}" target="_blank" class="btn-link btn-download">Baixar</a>
                            <a href="${jogo.link_youtube}" target="_blank" class="btn-link btn-youtube">YouTube</a>
                        </div>
                    </div>
                </div>
            `;
      divResultados.innerHTML += card;
    });
  } catch (erro) {
    loading.style.display = "none";
    divResultados.innerHTML = `<h3 style="color: var(--danger);">${erro.message}</h3>`;
  }
}
window.onload = inicializarApp;