import {Card, Tag} from "antd";

export const AnimeCard = (props) => {
    const {imageUrl, title, episodes, related = [], genre = "", animeId } = props;

    const overflowText = text => text.length > 21 ? `${text.substring(0, 21)}...` : text;

    const getSiteUrl = () => {
      if(related.length === 0) {
          return "https://myanimelist.net/"
      }
      return `https://myanimelist.net/anime/${animeId}`
    };

    const getTitle = () => {
        return title.replace("&#039", "\'")
    }

    return (
        <Card
            className="anime-card"
            onClick={() => window.open(getSiteUrl())}
            hoverable
            size={"small"}
            cover={<img className="anime-card-cover" alt={getTitle()}
                        src={(imageUrl || 'https://myanimelist.cdn-dena.com/images/anime/12/35893.jpg')
                            .replace("myanimelist.cdn-dena.com", "cdn.myanimelist.net")}
                        width={"180px"} height={"280px"}/>}
            >
            <div className="anime-title">{overflowText(getTitle())}</div>
            <span>
                <Tag className="genre-tag" color="magenta">{genre.split(',')[0] || "Comedy"}</Tag>
                {genre.split(',')[1] && <Tag className="genre-tag" color="blue">{genre.split(',')[1]}</Tag>}
            </span>
            <div className="anime-episodes">Episodes: {episodes}</div>
        </Card>
    )
};